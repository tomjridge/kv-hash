(** The core implementation is for a map [int->int]; here we add some
   additional support to enable a map [string->string].

To lookup a particular string key:

- hash the key to get an int
- use an int->int map to lookup the value_i (value as int)
- value_i is an index into a table of string values, implemented as a
  single file of all values, and an offset into that file

To add a key,value:

- hash the key to get an int
- add the value to the values file to get an offset
- add (hash,offset) to the int->int map

To delete a key:

- hash the key to get an int
- delete the int from the int->int map

NOTE the old value still remains in the values file. Whether this is a
problem depends on your application: if you often delete keys there
may be substantial overhead in the values file.

*)

open Util

module type VALUES = 
sig
  type t 
  val append_value : t -> string -> int
  val read_value : t -> off:int -> string
  val create : fn:string -> t
  val open_ : fn:string -> t
  val flush : t -> unit
  val close : t -> unit
end

module Values : VALUES = struct
  
  (* NOTE to avoid issues with torn writes when using mmap, we default
     to the standard pread/pwrite interface; perhaps better to use
     mmap and avoid writing over page boundaries? *)

  (* ASSUMES safe to use in and out channels on a single underlying file *)
  type t = {
    fn   : string;
    ch_r : Stdlib.in_channel;
    ch_w : Stdlib.out_channel;
  }

  (* we implement string read/write using basic in/out channel
     functions which use marshalling; FIXME format not stable long
     term *)

  (* NOTE because we are using marshalling, append_value and
     read_value are polymorphic in the value type, so we don't have to
     specialize to strings here; may be useful if the value type is
     something other than string *)
  let append_value t (v:string) = 
    let pos = pos_out t.ch_w in
    output_value t.ch_w v;
    flush t.ch_w;
    pos

  let read_value t ~off : string = 
    seek_in t.ch_r off;
    input_value t.ch_r

  let create ~fn = 
    (* make sure the file exists, and is of length 0 *)
    let fd = Unix.(openfile fn [O_CREAT;O_RDWR;O_TRUNC] 0o640) in
    Unix.close fd;
    let ch_r = open_in fn in
    let ch_w = open_out fn in
    { fn; ch_r; ch_w }

  let open_ ~fn = 
    let ch_r = open_in fn in
    let ch_w = open_out fn in
    { fn; ch_r; ch_w }

  let flush t = flush t.ch_w

  let close t = 
    flush t;
    close_in_noerr t.ch_r;
    close_out_noerr t.ch_w;
    ()
end

module type INT_MAP = sig 
    type t
    val find_opt : t -> int -> int option
    val insert   : t -> int -> int -> unit
    val delete   : t -> int -> unit
  end

type 'int_map t = {
  values: Values.t;
  int_map: 'int_map;
  deleted: (string,unit)Hashtbl.t (* FIXME hack to support delete quickly *)
}
  
module With_int_map(Int_map:INT_MAP) = struct

  let hash (s:string) = 
    let h = XXHash.XXH64.hash s |> Int64.to_int in
    let h = abs h in
    assert(h>=0);
    h

  let find_opt t k = 
    trace(fun () -> Printf.sprintf "%s: start\n" __FUNCTION__);
    let k' = hash k in
    begin
      (Hashtbl.find_opt t.deleted k) |> function 
      | Some () ->  None
      | None -> 
        Int_map.find_opt t.int_map k' |> function
        | None -> None
        | Some v' -> 
          Some(Values.read_value t.values ~off:v')
    end |> fun r -> 
    trace(fun () -> Printf.sprintf "%s: end\n" __FUNCTION__);
    r

  let insert_hashed t hash v = 
    trace(fun () -> Printf.sprintf "%s: start\n" __FUNCTION__);
    let k' = hash in
    Values.append_value t.values v |> fun off -> 
    Int_map.insert t.int_map k' off;
    trace(fun () -> Printf.sprintf "%s: end\n" __FUNCTION__);
    ()

  let insert t k v =
    trace(fun () -> Printf.sprintf "%s: start\n" __FUNCTION__);
    (Hashtbl.remove t.deleted k);
    let k' = hash k in
    insert_hashed t k' v;
    trace(fun () -> Printf.sprintf "%s: end\n" __FUNCTION__);
    ()

  let delete t k =
    Hashtbl.replace t.deleted k ()
(*
    let k' = hash k in
    Int_map.delete t.int_map k'
*)

  (* for batch, we need the hash and insert_hashed functions so we can
     pre-sort ops by hash of key, which improves subsequent
     performance; for deletes, we just accumulate; we assume there are
     no duplicate keys in ops *)
  (* FIXME we don't have to sort... it is enough to partition using
     the top-level partition, which should be much quicker *)
  let batch t ops =
    warn (fun () -> Printf.sprintf "%s: start\n" __FUNCTION__);
    ops |> List.filter_map (function
        | (k,`Insert v) -> Some(hash k,v)
        | (k,`Delete) -> delete t k; None) |> fun inserts -> 
    let t1 = Unix.time () in
    inserts |> List.sort (fun (h1,_) (h2,_) -> Int.compare h1 h2) |> fun inserts -> 
    let t2 = Unix.time () in
    Printf.printf "Sort took %f\n%!" (t2 -. t1);
    inserts |> List.iter (fun (h,v) -> insert_hashed t h v);
    warn (fun () -> Printf.sprintf "%s: end\n" __FUNCTION__);
    

    
    
    

end

(** Putting it all together.

To create given filename fn:
- create the int->int map on fn
- create the values map on fn.values


 *)
module Make_1 = struct

  module Config = struct
    (* 4096 blk_sz; 512 ints in total; 510 ints for unsorted and
       sorted; 255 kvs for unsorted and sorted *)

    let max_unsorted = 10
    let max_sorted = 255 - max_unsorted
    let blk_sz = 4096
  end

  module Int_map = Partition.Make(Config)

  module With_int_map_ = With_int_map(Int_map)

  let create ~fn = 
    trace(fun () -> Printf.sprintf "%s: start\n" __FUNCTION__);
    Int_map.create ~fn ~n:10_000 |> fun int_map -> 
    let values = Values.create ~fn:(fn ^".values") in
    let deleted = Hashtbl.create 1000 in
    trace(fun () -> Printf.sprintf "%s: end\n" __FUNCTION__);
    { values; int_map; deleted }

  (* let open_ ~fn = failwith "FIXME" *)
    
  include With_int_map_  
      
  let close t = 
    Values.close t.values;
    Int_map.close t.int_map
      
  type nonrec t = Int_map.t t
end


module type S = String_string_map_intf.S

module Make_2 : S = Make_1
