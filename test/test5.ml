(** Test of fork with shared mmap *)

open Kv_hash.Private.Util

let trace (_f:unit -> string) = ()

let h_len = 1024

type t = {
  fd:Unix.file_descr;
  m:(char,Bigarray.int8_unsigned_elt) Mmap.t;
  h:(char,Bigarray.int8_unsigned_elt,Bigarray.c_layout)Bigarray.Array1.t;  
}

let init = 
  let fn = "test.pip" in
  let fd = Unix.(openfile fn [O_CREAT;O_TRUNC;O_RDWR] 0o640) in
  let m = Mmap.of_fd fd Bigarray.char in
  (* initialize *)
  Mmap.sub m ~off:0 ~len:h_len |> fun h -> 
  {fd;m;h}
  
type header = {
  mutable len:int; (* length of valid data part, including first 1024 bytes *)
  mutable generation: int;
}

let write_header t (h:header) = 
  Marshal.(to_string h [No_sharing]) |> fun s -> 
  Bigstringaf.blit_from_string s ~src_off:0 t.h ~dst_off:0 ~len:(String.length s);
  ()

let read_header t : header =   
  Bigstringaf.to_string t.h |> fun s -> 
  Marshal.(from_string s 0)

let buffer_len = 256

(** Read an int at offset; off is updated; int is returned *)
let read_string t (off:int ref) : string =
  Mmap.sub t.m ~off:0 ~len:(!off+buffer_len) |> fun ba -> 
  Bin_prot.Read.bin_read_string ba ~pos_ref:off

(** Write an int at offset off; return new offset *)
let write_string m ~off ~str =
  Mmap.sub m ~off:0 ~len:(off+buffer_len) |> fun ba -> 
  Bin_prot.Write.bin_write_string ba ~pos:off str |> fun off' -> 
  off'

  

module Writer = struct
  type nonrec t = {
    t:t;
    h:header;
    (* NOTE we write at h.off *)
  }
end

let print_every = 1_000_000

include struct
  open Writer

  let append_int w k = 
    write_string w.t.m ~off:w.h.len ~str:(string_of_int k) |> fun off' -> 
    w.h.len <- off';
    write_header w.t w.h;
    ()

  let writer () = 
    let count = ref 0 in
    let h = { len=1024; generation=0 } in
    let w = { t=init; h } in
    Printf.printf "Writer thread starts\n%!";
    write_header w.t h;
    while(true) do      
      let k = !count in
      incr count;
      trace(fun () -> Printf.sprintf "Writing     %d\n%!" k);
      (if k mod print_every = 0 then Printf.printf "Writer      %d\n%!" k);
      append_int w k;
      Thread.yield()
    done
end

module Reader = struct  
  type nonrec t = {
    t:t;
    off:int ref; (* where we have read to so far in m *)
  }
end
open Reader

let reader () = 
  let r = { t=init; off=ref 1024 } in
  Printf.printf "Reader thread starts\n%!";
  while(true) do
    read_header r.t |> fun h -> 
    () |> iter_k (fun ~k:kont () -> 
        match !(r.off) < h.len with
        | false -> 
          trace(fun () -> Printf.sprintf "Reader read nothing\n%!");
          Thread.yield();
          ()
        | true -> 
          read_string r.t r.off |> fun s -> 
          let k = int_of_string s in
          trace(fun () -> Printf.sprintf "Reader read %d\n%!" k);
          (if k mod print_every = 0 then Printf.printf "Reader read %d\n%!" k);
          kont ());
  done
  
let main () =
  let i = Unix.fork () in
  (* initialize header *)
  write_header init {len=0;generation=0};
  match i with 
  | 0 -> 
    (* child *)
    reader ()
  | _pid -> 
    (* parent, child is pid *)
    writer ()

let _ = main ()      
