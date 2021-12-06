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
  let ( || ) e f = 
    match e with 
    | Error e -> Error e 
    | Ok () -> f ()

  let ppos_ptr = 8
  let data_ptr = 16
  let ppos = 16 (* the position of ppos in memory; starts at pos 16,
                   ie at beginning of data *)

  let int_to_bytes i = 
    let bs = Bytes.create 8 in
    Bytes.set_int64_be (* FIXME le? *) bs 0 (Int64.of_int i);
    bs

  let bytes_to_int bs =
    assert(Bytes.length bs = 8);
    Bytes.get_int64_be bs 0 |> Int64.to_int


  (* check that the fn looks correct; return the ppos *)
  let check_format_and_return_ppos ~header fn = 
    assert(Sys.file_exists fn);    
    (* FIXME possible gap where log file is deleted? *)
    let ic = open_in_bin fn in
    (* check length *)
    begin 
      let ic_len = in_channel_length ic in
      match ic_len >= data_ptr with
      | true -> Ok ()
      | false -> 
        let s = 
          Printf.sprintf "%s: file %s is less than %d bytes, and so \
                          cannot be a log file" 
            (log_location __LINE__) fn data_ptr
        in
        Log.err (fun m -> m "%s" s);
        close_in_noerr ic;
        Error (`E_short s)
    end || fun () -> 
      (* check_header *) 
      let buf8 = Bytes.create 8 in
      begin 
        assert(pos_in ic = 0);
        really_input ic buf8 0 8;
        match buf8 = header with
        | true -> Ok ()
        | false -> 
          let s = 
            Printf.sprintf "%s: file %s does not have the correct \
                            header for a log file (expected %S, got %S)" 
              (log_location __LINE__) fn (Bytes.to_string header) (Bytes.to_string buf8)
          in
          Log.err (fun m -> m "%s" s);
          close_in_noerr ic;
          Error (`E_header s)
      end || fun () -> 
        (* check_ppos *)
        let buf8 = Bytes.create 8 in
        let ppos_ref = ref 0 in
        begin 
          assert(pos_in ic = 8);
          really_input ic buf8 0 8;
          let ppos = bytes_to_int buf8 in
          ppos_ref := ppos;
          let ic_len = in_channel_length ic in
          match ppos >= data_ptr && ppos <= ic_len with
          | true -> Ok ()
          | false -> 
            let s = 
              Printf.sprintf "%s: file %s has a ppos value of %d \
                              which is beyond the length %d of the \
                              file (or less than data_ptr)" 
                (log_location __LINE__) fn ppos ic_len
            in
            Log.err (fun m -> m "%s" s);
            close_in_noerr ic;
            Error (`E_ppos_invalid s)
        end || fun () -> 
          assert(!ppos_ref >= data_ptr);
          Ok (ic,!ppos_ref)


  module type HEADER = sig val header: (*8*)bytes end

  module Make_w(H:HEADER) = struct
    let _ = assert(Bytes.length H.header = 8) (* to match length_ptr *)

    type t = { 
      fn           : string;
      oc           : Stdlib.out_channel; 
      mutable pos  : int
    }

    (* FIXME this is also problematic if we crash during init; so
       perhaps we need to do everything in a tmp file *)
    let init oc = 
      seek_out oc 0;
      output_bytes oc H.header;
      assert(pos_out oc = ppos_ptr);
      output_bytes oc (int_to_bytes data_ptr);
      assert(pos_out oc = data_ptr);
      flush oc;
      ()
    
    let perm = 0o640 (* user rw-x; g r-wx; o -rwx *)

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
      end || fun () -> 
        begin match not create_excl && not file_exists with
          | true -> 
            (* file doesn't exist, and we didn't try to create it *)
            let s = Printf.sprintf "%s: file %s does not exist" 
                (log_location __LINE__) fn 
            in
            Log.err (fun m -> m "%s" s);
            Error (`E_noent s)
          | false -> Ok () 
        end || fun () -> 
          (* at this point, if create_excl is set, then the file doesn't
             exist; so create it *)
          if create_excl then begin 
            assert(not file_exists);
            let fn_tmp = fn^".tmp" in
            (* create the file; use a temporary file and rename over
               original; this ensures that the reader never sees a
               part-initialized file *)
            let oc = Stdlib.open_out_gen [Open_wronly; Open_creat; Open_trunc] perm fn_tmp in
            init oc;
            close_out_noerr oc;
            (* FIXME at this point we also want to flush the dir; but
               OCaml doesn't seem to have this functionality in stdlib,
               unless we actually open an fd on the dir itself and fsync
               that *)
            Sys.rename fn_tmp fn;
            (* FIXME dir sync *)
          end;
          (* at this point, the file definitely exists; if it was created,
             it is in the correct format *)
          check_format_and_return_ppos ~header:H.header fn |> function
          | Error e -> Error e
          | Ok (ic,ppos) -> 
            assert(ppos >= data_ptr);
            close_in_noerr ic;
            (* FIXME not sure I like this channel interface; FIXME prefer std unix interface with really_pread etc *)
            let oc = Stdlib.open_out_gen [Open_wronly;Open_binary] perm fn in
            seek_out oc ppos;
            Ok { fn; oc; pos=ppos }

    let pos t = t.pos

    let append t s = 
      (* NOTE this seek_out redundant if we maintain invariant that the
         oc pos is always equal to t.pos *)
      assert(t.pos >= data_ptr);
      seek_out t.oc t.pos;
      output_value t.oc (s:string);
      t.pos <- pos_out t.oc;
      ()  

    let flush ?(sync_after_ppos=true) t = 
      flush t.oc;
      seek_out t.oc ppos_ptr;
      output_bytes t.oc (int_to_bytes t.pos);
      (if sync_after_ppos then flush t.oc);
      seek_out t.oc t.pos;
      ()

    let close t = 
      flush t;
      close_out_noerr t.oc;
      ()

  end

  module Header_ = struct let header = "log_file" |> Bytes.of_string end

  module Log_file_w = Make_w(Header_)


  module Make_r(H:HEADER) = struct
    let _ = assert(Bytes.length H.header = 8) (* to match length_ptr *)

    type t = { ic: in_channel; mutable ppos:int; mutable buf8:bytes(*8*) }

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
      end || fun () -> 
        (* at this point, file exists; since we use the rename trick
           to create the file from the [log_file_w], it must be
           correctly initialized *)
        check_format_and_return_ppos ~header:H.header fn |> function
        | Error e -> Error e
        | Ok (ic,ppos) -> 
          Ok { ic; ppos; buf8=Bytes.create 8 }

    let init_pos = data_ptr

    let update_ppos t = 
      (* read *)
      seek_in t.ic ppos_ptr;      
      really_input t.ic t.buf8 0 8;
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
      seek_in t.ic !pos;
      let s : string = input_value t.ic in
      pos := pos_in t.ic;
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

    let close t = 
      close_in_noerr t.ic;
      ()
        
  end

  module Log_file_r = Make_r(Header_)  

end (* Private *)

module Log_file_w : LOG_FILE_W = Private.Log_file_w 

module Log_file_r : LOG_FILE_R = Private.Log_file_r


module Test(S:sig val limit : int val fn : string end) = struct

  open S

  (** Create log file, then write entries until limit reached *)
  let run_writer () =
    assert(not (Sys.file_exists fn));
    let t = Log_file_w.open_ ~create_excl:true fn |> discard_err in
    let n = ref 0 in
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
