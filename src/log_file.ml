(** Support for append-only log files, with header field containing
    synced length, and fsync used to ensure crash-resistance.

The problem we try to address are outlined here: https://tom-ridge.blogspot.com/2021/11/how-to-safely-append-to-file-can-we.html

We have a file format as follows:
{v
+------------------------------------------------+
| Header id | Ppos   | Data... | Pending data... |
+------------------------------------------------+
            ^        ^         ^                 ^
         ppos_ptr    data_ptr  |                 pos
         (=8)        (=16)     ppos
v}

The ppos (persistent position) field is recorded in the file
itself. It records the on-disk persistent position that was last
flushed. The scenario we need to avoid is:

  - append data
  - (no fsync)
  - update ppos

The danger is that the update of ppos might occur before the append of the data.

On the other hand, issuing an fsync after each append may give poor performance.

ppos cannot be updated without first issuing an fsync to ensure the
appended data is on disk. Then after ppos is written, we have the
option of forcing the write of ppos to disk as well. If we don't mind losing
some recently written data, we can avoid an fsync at this point.

The default behaviour we employ is: appends do not automatically force
a ppos update. Instead, the user must call sync occasionally to
force writes to disk.

*)

(** Errors that can arise on opening a log file *)
module E_open = struct
  type e_open = [
    | `E_exist of string        (** File already exists, and we expected it did not *)
    | `E_noent of string        (** File doesn't exist, and we didn't ask to create it *)
    | `E_short of string        (** File too short *)
    | `E_header of string       (** Incorrect file header *)
    | `E_ppos_invalid of string (** ppos points beyond end of file,
                                   breaking invariant (or: ppos points
                                   into header) *)
  ]
  (** The type of errors when opening. NOTE: these errors should be
      impossible if the code is correct; so when opening, use discard_err
      to convert to an exception *)

  let to_string = function
    | `E_exist s | `E_noent s | `E_short s | `E_header s | `E_ppos_invalid s -> failwith s

  let discard_err : ('a,e_open) result -> 'a = function
    | Ok x -> x
    | Error e -> failwith (to_string e)
    (** Convert a [(t,e_open) result] to a t, using [failwith] for the
       error cases. Since the error cases should be impossible with
       correct code, this is the usual way to handle the result type
       from [open_]. *)
end
open E_open


module type LOG_FILE_W = sig

  type t
  (** The handle for the log file *)

  val open_: create_excl:bool -> string -> (t,e_open)result
  (** [create_excl] will cause [open_] to create the file if it
     doesn't exist, and fail if the file does exist; use {!
     E_open.discard_err } to eliminate the error result type. *)

  val pos: t -> int
  (** The position at which the next append will occur. *)

  val append: t -> string -> unit
  (** Append a string to the end of the file. *)

(* FIXME perhaps for future
  val append_bytes: t -> bytes -> unit
  (** Append a byte sequence to the end of the file. Repeatedly using
     the same buffer to write bytes avoids excessive GC that might
     occur when using strings. *)
*)

  val flush: ?sync_after_ppos:bool -> t -> unit
  (** Ensure appends are pushed to disk, and then issue an fsync; then
     flush ppos (always) and fsync (if flag set, which is the default,
     although more costly) *)

  val close: t -> unit
  (** Call flush with [sync_after_ppos] set, then close output channel. *)

end

(** Read-only log file; NOTE we only read upto the ppos recorded on
   disk, but there may be valid entries beyond the ppos that haven't
   been flushed yet *)
module type LOG_FILE_R = sig

  type t

  val open_: ?wait:bool -> string -> (t,e_open)result
  (** Flag [wait] indicates that we should wait for the file to exist,
     and be of the right size, before attempting to read; otherwise,
     if the file is non-existent, we return with an error *)

  val init_pos: int
  (** The initial position (after the header) from which data can be read *)

  val ppos: t -> int
  (** The persistent position recorded on disk; can be changed by a
     [log_file_w] *)

  val can_read: t -> pos:int ref -> bool
  (** If [!pos < ppos], return [true] to indicate we can read more strings *)

  val read: t -> pos:int ref -> string
  (** Read a single entry; must have !pos <= ppos *)

  val read_from: t -> pos:int ref -> string list
  (** Read all entries upto current ppos, as a (potentially very
     large) list; NOTE that ppos may be updated in the meantime, in
     which case there may be more entries to read *)

  val close: t -> unit

end

module Private = struct

  (** An initial log prefix to indicate where we are reporting from;
     probably better to use the log library inbuilt functionality for
     this *)
  let log_location line =
    Printf.sprintf "%s %s %d"
      Config.Consts.library_name
      __FILE__
      line

  (* NOTE this just makes the error handling slightly easier to
     read: [e && fun () -> ...] is [e] if [e] is an [Error e'],
     otherwise the result of the function *)
  let ( ||| ) e f =
    match e with
    | Error e -> Error e
    | Ok () -> f ()

  let ppos_ptr = 8
  let data_ptr = 16
  let ppos = 16 (* the position of ppos in memory; starts at pos 16,
                   ie at beginning of data *)

  let _ = assert(Sys.word_size = 64 || failwith "Need 64bit platform")

  let int_to_bytes i =
    let bs = Bytes.create 8 in
    Bytes.set_int64_be (* FIXME le? *) bs 0 (Int64.of_int i);
    bs

  let bytes_to_int bs =
    assert(Bytes.length bs = 8);
    Bytes.get_int64_be bs 0 |> Int64.to_int

  let len8 = 8

  (* check that the fn looks correct; return the ppos *)
  let check_format_and_return_ppos ~header ~fn ~fd =
    (* check length *)
    let fd_len = Unixfile.fstat_size fd in
    begin
      match fd_len >= data_ptr with
      | true -> Ok ()
      | false ->
        let s =
          Printf.sprintf "%s: file %s is less than %d bytes, and so \
                          cannot be a log file"
            (log_location __LINE__) fn data_ptr
        in
        Log.err (fun m -> m "%s" s);
        Unixfile.close_noerr fd;
        Error (`E_short s)
    end ||| fun () ->
      (* check_header *)
      let buf8 = Bytes.create len8 in
      begin
        ignore(Unixfile.pread_all ~fd ~off:0 ~buf:buf8 ~buf_off:0 ~len:len8);
        match buf8 = header with
        | true -> Ok ()
        | false ->
          let s =
            Printf.sprintf "%s: file %s does not have the correct \
                            header for a log file (expected %S, got %S)"
              (log_location __LINE__) fn (Bytes.to_string header) (Bytes.to_string buf8)
          in
          Log.err (fun m -> m "%s" s);
          Unixfile.close_noerr fd;
          Error (`E_header s)
      end ||| fun () ->
        (* check_ppos *)
        let buf8 = Bytes.create len8 in
        let ppos_ref = ref 0 in
        begin
          ignore(Unixfile.pread_all ~fd ~off:ppos_ptr ~buf:buf8 ~buf_off:0 ~len:len8);
          let ppos = bytes_to_int buf8 in
          ppos_ref := ppos;
          match ppos >= data_ptr && ppos <= fd_len with
          | true -> Ok ()
          | false ->
            let s =
              Printf.sprintf "%s: file %s has a ppos value of %d \
                              which is beyond the length %d of the \
                              file (or less than data_ptr)"
                (log_location __LINE__) fn ppos fd_len
            in
            Log.err (fun m -> m "%s" s);
            Unixfile.close_noerr fd;
            Error (`E_ppos_invalid s)
        end ||| fun () ->
          assert(!ppos_ref >= data_ptr);
          Ok (!ppos_ref)


  let perm = 0o640 (* user rw-x; g r-wx; o -rwx *)

  let init_pos = data_ptr

  let len_65536 = 65536

  module type HEADER = sig val header: (*8*)bytes end

  module Marshal = struct end (* make sure we don't use stdlib Marshal *)

  module Marshal_ = struct

    let max_string_len = Int32.(max_int |> to_int)

    (** Marshal a string to a buffer; first four bytes is string
       length; rest is the string; string length must be representable
       as an int32; use buf if possible, otherwise create a new buf
       and return that; alsways use the returned buf (don't rely on
       the argument buf being used); returned buf from 0 to |s|+4
       contains the encoded bytes; NOTE inefficient for short strings,
       since we need 4 bytes to encode the length; could be improved
       FIXME? by using a bin_prot like encoding of strings *)
    let output_string ~buf s =
      let len = String.length s in
      assert(len <= max_string_len);
      let buf =
        match len+4 <= Bytes.length buf with
        | true -> buf
        | false -> Bytes.create (len+4)
      in
      BytesLabels.set_int32_be buf 0 (Int32.of_int len);
      BytesLabels.blit_string ~src:s ~src_pos:0 ~dst:buf ~dst_pos:4 ~len;
      buf (* NOTE valid bytes are from 0 to |s|+4 *)

    let output_string_fd ~fd ~off ~buf s =
      let buf = output_string ~buf s in
      let len = String.length s +4 in
      ignore(Unixfile.pwrite_all ~fd ~off ~buf ~buf_off:0 ~len : int);
      len

    (** buf is a buffer that will be used to hold the string while
       decoding (if the buffer is big enough; otherwise we create a
       new buffer); buf must have length at least 4 *)
    let input_string_fd ~fd ~off ~buf =
      let len_buf = Bytes.length buf in
      assert(len_buf >= 4);
      ignore(Unixfile.pread_all ~fd ~off ~buf ~buf_off:0 ~len:4 : int);
      let len = Bytes.get_int32_be buf 0 |> Int32.to_int in
      let buf =
        match len <= len_buf with
        | true -> buf
        | false -> Bytes.create len
      in
      ignore(Unixfile.pread_all ~fd ~off:(off+4) ~buf ~buf_off:0 ~len : int);
      let s = Bytes.to_string (BytesLabels.sub buf ~pos:0 ~len) in
      s

    (** After reading a string from pos, return the next possible pos *)
    let next_off_delta s = 4 + String.length s

  end

  module Make_w(H:HEADER) = struct

    let _ = assert(Bytes.length H.header = len8) (* to match length_ptr *)

    type t = {
      fn          : string;
      fd          : Unix.file_descr;
      mutable pos : int;
      (** Position at end of the file, where we append new data. *)
      buf         : bytes;
      (** Buffer for storing marshalled values before writing to file *)
    }

    let write_header fd =
      ignore(Unixfile.pwrite_all ~fd ~off:0 ~buf:H.header ~buf_off:0 ~len:len8)

    let write_ppos ~fd ~ppos =
      ignore(Unixfile.pwrite_all ~fd ~off:ppos_ptr ~buf:(int_to_bytes ppos) ~buf_off:0 ~len:len8)


    (* FIXME this is also problematic if we crash during init; so
       perhaps we need to do everything in a tmp file *)
    let init fd =
      write_header fd;
      write_ppos ~fd ~ppos:init_pos; (* initial ppos is init_pos = data_ptr *)
      Unixfile.fsync fd;
      ()

    let open_ ~create_excl fn =
      let file_exists = Sys.file_exists fn in
      begin match create_excl && file_exists with
        | true ->
          let s = Printf.sprintf "%s: file %s exists"
              (log_location __LINE__) fn
          in
          Log.err (fun m -> m "%s" s);
          Error (`E_exist s)
        | false -> Ok ()
      end ||| fun () ->
        begin match not create_excl && not file_exists with
          | true ->
            (* file doesn't exist, and we didn't try to create it *)
            let s = Printf.sprintf "%s: file %s does not exist"
                (log_location __LINE__) fn
            in
            Log.err (fun m -> m "%s" s);
            Error (`E_noent s)
          | false -> Ok ()
        end ||| fun () ->
          (* at this point, if create_excl is set, then the file doesn't
             exist; so create it *)
          let fn_tmp = fn^".tmp" in
          let fd =
            match create_excl with
            | true -> begin
                assert(not file_exists);
                (* create the file; use a temporary file and rename over
                   original; this ensures that the reader never sees a
                   part-initialized file *)
                let fd = Unix.(openfile fn_tmp [O_RDWR; O_CREAT; O_EXCL] perm) in
                init fd;
                (* FIXME at this point we also want to flush the dir; but
                   OCaml doesn't seem to have this functionality in stdlib,
                   unless we actually open an fd on the dir itself and fsync
                   that *)
                Sys.rename fn_tmp fn;
                (* FIXME dir sync *)
                fd
              end
            | false ->
              let fd = Unix.(openfile fn [O_RDWR] perm) in
              fd
          in
          (* at this point, the file definitely exists; if it was created,
             it is in the correct format FIXME pass fd from prev *)
          check_format_and_return_ppos ~header:H.header ~fn ~fd |> function
          | Error e -> Error e
          | Ok ppos ->
            assert(ppos >= data_ptr);
            Ok { fn; fd; pos=ppos; buf=Bytes.create len_65536 }

    let pos t = t.pos

    let append t s =
      (* NOTE this seek_out redundant if we maintain invariant that the
         oc pos is always equal to t.pos *)
      assert(t.pos >= data_ptr);
      let len = Marshal_.output_string_fd ~fd:t.fd ~off:t.pos ~buf:t.buf s in
      t.pos <- t.pos + len;
      ()

    let flush ?(sync_after_ppos=true) t =
      Unixfile.fsync t.fd;
      ignore (Unixfile.pwrite_all ~fd:t.fd ~off:ppos_ptr ~buf:(int_to_bytes t.pos) ~buf_off:0 ~len:len8);
      (if sync_after_ppos then Unixfile.fsync t.fd);
      ()

    let close t = Unixfile.close_noerr t.fd

  end

  module Header_ = struct let header = "log_file" |> Bytes.of_string end

  module Log_file_w = Make_w(Header_)

  module Make_r(H:HEADER) = struct
    let _ = assert(Bytes.length H.header = 8) (* to match length_ptr *)

    type t = {
      fd           : Unix.file_descr;
      mutable ppos : int;
      (** Last ppos read from disk *)
      buf8         : bytes(*8*);
      buf          : bytes;
    }

    let open_ ?(wait=true) fn =
      begin match Sys.file_exists fn with
      | false -> (
          match wait with
          | false ->
            (* file doesn't exist *)
            let s = Printf.sprintf "%s: file %s does not exist"
                (log_location __LINE__) fn
            in
            Log.err (fun m -> m "%s" s);
            Error (`E_noent s)
          | true ->
            while not (Sys.file_exists fn) do
              Log.info (fun m -> m "Waiting 1s for file %s to be created." fn);
              Thread.delay 1.0
            done;
            Ok ())
      | true -> Ok ()
      end ||| fun () ->
        (* at this point, file exists; since we use the rename trick
           to create the file from the [log_file_w], it must be
           correctly initialized *)
        let fd = Unix.(openfile fn [O_RDWR] perm) in
        check_format_and_return_ppos ~header:H.header ~fn ~fd |> function
        | Error e -> Error e
        | Ok ppos ->
          Ok { fd; ppos; buf8=Bytes.create 8; buf=Bytes.create len_65536 }

    let init_pos = init_pos

    let update_ppos t =
      (* read *)
      ignore(Unixfile.pread_all ~fd:t.fd ~off:ppos_ptr ~buf:t.buf8 ~buf_off:0 ~len:len8);
      (* update in t *)
      t.ppos <- bytes_to_int t.buf8;
      ()

    let ppos t =
      update_ppos t;
      t.ppos

    let can_read t ~pos =
      update_ppos t;
      !pos < t.ppos

    let read t ~pos =
      assert(can_read t ~pos);
      (* the problem is how to marshal from an fd, when we don't know
         the length; this is covered in the Marshal module via
         header_size, data_size etc; given the complexity, it makes
         sense to just implement our own marshal here *)
      let s = Marshal_.input_string_fd ~fd:t.fd ~off:!pos ~buf:t.buf in
      let delta = Marshal_.next_off_delta s in
      pos := !pos + delta;
      s

    let read_from t ~pos =
      update_ppos t;
      let ppos = t.ppos in
      [] |> Util.iter_k (fun ~k xs ->
          match !pos < ppos with
          | false -> List.rev xs
          | true ->
            read t ~pos |> fun x ->
            k (x::xs))
    (* NOTE on-disk ppos may be updated in the meantime *)

    let close t = Unixfile.close_noerr t.fd

  end

  module Log_file_r = Make_r(Header_)

end (* Private *)

module Log_file_w : LOG_FILE_W = Private.Log_file_w

module Log_file_r : LOG_FILE_R = Private.Log_file_r


module Test(S:sig val limit : int val fn : string end) = struct

  open S

  (** Create log file, then write entries until limit reached *)
  let run_writer () =
    let t = Log_file_w.open_ ~create_excl:(not (Sys.file_exists fn)) fn |> discard_err in
    let n = ref 0 in
    (* If we exit early (eg by user pressing ctrl+c) we print the position we reached *)
    Stdlib.at_exit (fun () -> Printf.printf "Writer reached position %d%!" !n);
    while !n < limit do
      Log_file_w.append t (string_of_int !n);
      Log_file_w.flush t;
      incr n
    done;
    Log_file_w.close t;
    ()

  (** Wait for log file, then print entries, until limit reached *)
  let run_reader () =
    let t = Log_file_r.open_ ~wait:true fn |> discard_err in
    let n = ref 0 in
    let pos = ref Log_file_r.init_pos in
    while !n < limit do
      if Log_file_r.can_read t ~pos then begin
        let xs = Log_file_r.read_from t ~pos in
        List.iter print_endline xs;
        n:=!n + List.length xs
      end
    done;
    Log_file_r.close t;
    ()

end
