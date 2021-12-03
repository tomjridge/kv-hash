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

module type LOG_FILE_W = sig

  type t

  val open_: create_excl:bool -> string -> t
  (** create_excl will cause [open_] to create the file if it doesn't
      exist, and fail if the file does exist *)
    
  val pos: t -> int  
  (** The position at which next append will occur *)

  val append: t -> string -> unit
  (** Append a string to the end of the file. *)

  val flush: ?flush_ppos:bool -> t -> unit  
  (* ensure changes pushed to disk; also flush ppos if flag set, which
     is the default, although more costly *)

end

module Private = struct
  module Log_file_w = struct

    type t = { 
      fn           : string;
      oc           : Stdlib.out_channel; 
      mutable pos  : int
    }

    let header = "log_file" |> Bytes.of_string
    let _ = assert(Bytes.length header = 8) (* to match length_ptr *)

    let ppos_ptr = 8
    let data_ptr = 16
    let ppos = 16 (* the position of ppos in memory; starts at pos 16,
                     ie at beginning of data *)

    let int_to_bytes i = 
      let bs = Bytes.create 4 in
      Bytes.set_int64_be (* FIXME le? *) bs 0 (Int64.of_int i);
      bs

    let bytes_to_int bs =
      assert(Bytes.length bs = 4);
      Bytes.get_int64_be bs 0 |> Int64.to_int

    (* FIXME this is also problematic if we crash during init; so
       perhaps we need to do everything in a tmp file *)
    let init oc = 
      seek_out oc 0;
      output_bytes oc header;
      assert(pos_out oc = ppos_ptr);
      output_bytes oc (int_to_bytes data_ptr);
      assert(pos_out oc = data_ptr);
      flush oc;
      ()

    (* check that the fn looks correct; return the ppos *)
    let check_format_and_return_ppos fn = 
      assert(Sys.file_exists fn);    
      let ic = open_in_bin fn in
      let ic_len = in_channel_length ic in
      let _check_length = 
        match ic_len >= data_ptr with
        | true -> () 
        | false -> 
          let s = Printf.sprintf "%s %s: file %s is less than %d bytes, and so cannot be a log file" 
              Config.Consts.library_name __MODULE__ fn data_ptr
          in
          Log.err (fun m -> m "%s" s);
          failwith s
      in
      let buf8 = Bytes.create 8 in
      let _check_header = 
        assert(pos_in ic = 0);
        really_input ic buf8 0 8;
        match buf8 = header with (* FIXME check this is the expected byte-by-byte comparison *)
        | true -> ()
        | false -> 
          let s = Printf.sprintf "%s %s: file %s does not have the correct header for a log file" 
              Config.Consts.library_name __MODULE__ fn 
          in
          Log.err (fun m -> m "%s" s);
          failwith s
      in        
      let buf4 = Bytes.create 4 in
      let ppos_ref = ref 0 in
      let _check_ppos =
        assert(pos_in ic = 8);
        really_input ic buf4 0 4;
        let ppos = bytes_to_int buf4 in
        ppos_ref := ppos;
        match ppos >= 16 && ppos <= ic_len with
        | true -> ()
        | false -> 
          let s = Printf.sprintf "%s %s: file %s has a ppos value of %d which is beyond the length %d of the file (or less than 16)" 
              Config.Consts.library_name __MODULE__ fn ppos ic_len
          in
          Log.err (fun m -> m "%s" s);
          failwith s
      in
      Stdlib.close_in_noerr ic;
      !ppos_ref


    let perm = 0o640 (* user rw-x; g r-wx; o -rwx *)

    let open_ ~create_excl fn = 
      let file_exists = Sys.file_exists fn in
      begin match create_excl && file_exists with 
        | true -> 
          let s = Printf.sprintf "%s %s: file %s exists" 
              Config.Consts.library_name __MODULE__ fn 
          in
          Log.err (fun m -> m "%s" s);
          failwith s
        | false -> () end;
      begin match not create_excl && not file_exists with
        | true -> 
          (* file doesn't exist, but we didn't try to create it *)
          let s = Printf.sprintf "%s %s: file %s does not exist" 
              Config.Consts.library_name __MODULE__ fn 
          in
          Log.err (fun m -> m "%s" s);
          failwith s
        | false -> () end;
      (* at this point, if create_excl is set, then the file doesn't
         exist; so create it *)
      if create_excl then begin 
        assert(not file_exists);
        let fn_tmp = fn^".tmp" in
        (* create the file; use a temporary file and rename over original *)
        let oc = Stdlib.open_out_gen [Open_wronly; Open_creat; Open_trunc] perm fn_tmp in
        init oc;
        close_out_noerr oc;
        (* FIXME at this point we also want to flush the dir; but OCaml
           doesn't seem to have this functionality in stdlib *)
        Sys.rename fn_tmp fn;
        (* FIXME dir sync *)
      end;
      (* at this point, the file definitely exists; if it was created,
         it is in the correct format *)
      let ppos = check_format_and_return_ppos fn in
      let oc = Stdlib.open_out fn in
      { fn; oc; pos=ppos }

    let pos t = t.pos

    let append t s = 
      (* NOTE this seek_out redundant if we maintain invariant that the
         oc pos is always equal to t.pos *)
      seek_out t.oc t.pos;
      output_string t.oc s;
      t.pos <- pos_out t.oc;
      ()  

    let flush ?(flush_ppos=true) t = 
      flush t.oc;
      if flush_ppos then begin
        seek_out t.oc ppos_ptr;
        output_bytes t.oc (int_to_bytes t.pos);
        flush t.oc;
        seek_out t.oc ppos
      end;
      ()

  end
end

module Log_file_w : LOG_FILE_W with type t = Private.Log_file_w.t = Private.Log_file_w 

