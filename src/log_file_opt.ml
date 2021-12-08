(** An optimistic log file. We write strings to a file, append
   only. For each string, we write a header consisting of: the length
   of the string, the xxhash of the contents.

On crash, we can recover all of the file to the last flush, and
   perhaps some entries after the last flush. *)
open Log_file
open E_open


module Private = struct

  let _ = assert(Sys.word_size = 64 || failwith "Need 64bit platform")

  let len8 = 8

  let len16 = 16

  let int64_to_bytes i ~buf ~buf_off =
    assert(Bytes.length buf >= 8);
    Bytes.set_int64_be buf buf_off (i : Int64.t);
    ()

  let int_to_bytes i ~buf ~buf_off =
    int64_to_bytes (Int64.of_int i) ~buf ~buf_off;
    ()

  let bytes_to_int64 ~buf ~buf_off = 
    assert(Bytes.length buf >= 8);
    Bytes.get_int64_be buf buf_off 

  let bytes_to_int ~buf ~buf_off =
    bytes_to_int64 ~buf ~buf_off |> Int64.to_int  


  module Writer (* : Log_file.LOG_FILE_W *) = struct

    type t = { oc: out_channel; buf16:bytes }

    (* Crash recovery: if the data in the file ends with invalid
       entries, we skip over these; for an incomplete entry (say,
       header written by no data) we fill the invalid data with
       garbage eg 0s, and continue writing new entries afterwards *)

    let open_ ~create_excl:_ fn =
      let oc = open_out_bin fn in
      Ok { oc; buf16=Bytes.create len16 }

    let pos t = pos_out t.oc

    let append_bytes t ~buf ~buf_off ~len = 
      output t.oc buf buf_off len    

    let append t s = 
      (* use t.buf16 to assemble header info: |s|,hash(s) *)
      let s_len = String.length s in
      int_to_bytes s_len ~buf:t.buf16 ~buf_off:0;
      let h = XXHash.XXH64.hash s in
      int64_to_bytes h ~buf:t.buf16 ~buf_off:8;
      (* write header *)
      append_bytes t ~buf:t.buf16 ~buf_off:0 ~len:16;
      (* write the string itself *)
      output_string t.oc s;
      ()

    let flush t = flush t.oc

    let close t = close_out_noerr t.oc    

  end


  module Reader = struct 

    type t = { fn:string; ic:in_channel; buf16:bytes }
             
    let open_ ?(wait=true) fn =
      ignore(wait);
      (* assume file exists for now *)
      let ic = open_in_bin fn in
      { fn; ic; buf16=Bytes.create 16 }

    let pos t = pos_in t.ic

    let can_read t = pos_in t.ic < in_channel_length t.ic

    (** Error returned for corrupt entry, presumably at or near end of
       file *)
    let read t =
      (* we remember pos, in case we need to reset later *)
      let pos = pos_in t.ic in
      match pos + 16 <= in_channel_length t.ic with
      | false -> Ok None
      | true -> 
        (* read the length *)
        really_input t.ic t.buf16 0 8;
        let len = bytes_to_int ~buf:t.buf16 ~buf_off:0 in
        match pos+16+len <= in_channel_length t.ic with
        | false -> 
          (* reset pos *)
          seek_in t.ic pos;
          Ok None
        | true -> 
          (* read hash *)
          really_input t.ic t.buf16 8 8;
          let h = bytes_to_int64 ~buf:t.buf16 ~buf_off:8 in
          (* read string *)
          let s = really_input_string t.ic len in
          (* check hash *)
          match XXHash.XXH64.hash s = h with
          | true -> Ok (Some s)
          | false -> (
              Printf.printf "%s: invalid entry for file %s at position %d\n" 
                __MODULE__
                t.fn
                pos;
              (* NOTE we don't reset the pos; one option is for
                 further new valid entries when the writer restarts;
                 another is for the writer just to start a new log and
                 signal this somehow to the reader *)
              Error ())

    let close t = close_in_noerr t.ic
          
  end

  let recover _fn = (* TODO *) ()

end
