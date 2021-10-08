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

module Partition_ = Partition.Partition_ii

(** A [string->string] map is a values file and an [int->int] map. The
   [int->int] map is really a map from hash(key) to offset(of value in
   values file). *)
type 'int_map t = {
  values: Values.t;
  mutable p_int_map: 'int_map; (* mutable because RO instances need to
                                sync this from disk after a merge *)
  (* deleted: (string,unit)Hashtbl.t; (\* FIXME hack to support delete quickly *\) *)
  cache: (string,string) Cache_2gen.t
}


(** What we need from the large [int->int] map *)
module type PHASH = sig 
    type t
    val find_opt : t -> int -> int option
    val insert   : t -> int -> int -> unit
    (* val delete   : t -> int -> unit FIXME currently hacked using lookaside hashtable *)
  end

module Cache = Cache_2gen
  
(* cache size *)
let const_1M = 1_000_000

module With_phash(Phash:PHASH) = struct

  let hash (s:string) = 
    let h = XXHash.XXH64.hash s |> Int64.to_int in
    let h = abs h in
    assert(h>=0);
    h

  let find_opt t k = 
    trace(fun () -> Printf.sprintf "%s: start\n" __FUNCTION__);
    (* first try to find in cache *)
    match Cache.find_opt' t.cache k with
    | Some (`Present v) -> (Some v)
    | Some `Absent -> None
    | None -> 
      let k' = hash k in
      begin
        Phash.find_opt t.p_int_map k' |> function
        | None -> 
          Cache.add t.cache k `Absent;
          None
        | Some v' -> 
          Values.read_value t.values ~off:v' |> fun v -> 
          Cache.add t.cache k (`Present v);
          Some v          
      end |> fun r -> 
      trace(fun () -> Printf.sprintf "%s: end\n" __FUNCTION__);
      r
      
  (* hash is the hash of k *)
  let insert_hashed t k hash v = 
    trace(fun () -> Printf.sprintf "%s: start\n" __FUNCTION__);
    Cache.add t.cache k (`Present v);
    let k' = hash in
    Values.append_value t.values v |> fun off -> 
    Phash.insert t.p_int_map k' off;
    trace(fun () -> Printf.sprintf "%s: end\n" __FUNCTION__);
    ()

  let insert t k v =
    trace(fun () -> Printf.sprintf "%s: start\n" __FUNCTION__);
    let k' = hash k in
    insert_hashed t k k' v;
    trace(fun () -> Printf.sprintf "%s: end\n" __FUNCTION__);
    ()

  (* for batch, we need the hash and insert_hashed functions so we can
     pre-sort ops by hash of key, which improves subsequent
     performance; for deletes, we just accumulate; we assume there are
     no duplicate keys in ops *)
  (* FIXME we don't have to sort... it is enough to partition using
     the top-level partition, which should be much quicker *)
  let batch t ops =
    trace (fun () -> Printf.sprintf "%s: start\n" __FUNCTION__);
    ops |> List.filter_map (function
        | (k,`Insert v) -> Some(k,hash k,v)
        | (_k,`Delete) -> failwith "Delete not supported") |> fun inserts -> 
    let t1 = Unix.time () in
    inserts |> List.sort (fun (_,h1,_) (_,h2,_) -> Int.compare h1 h2) |> fun inserts -> 
    let t2 = Unix.time () in
    (* FIXME also want to remove new inserts from deleted, and FIXME
       need to be sure order of deletes and inserts *)
    Printf.printf "Sort took %f\n%!" (t2 -. t1);
    inserts |> List.iter (fun (k,h,v) -> insert_hashed t k h v);
    (* this is a good place to trim the cache... assuming we always go via batch *)
    Cache.maybe_trim t.cache ~young_sz:const_1M ~old_sz:const_1M;
    trace (fun () -> Printf.sprintf "%s: end\n" __FUNCTION__);

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

  module Phash = Persistent_hashtable.Make_2(Config)

  module With_phash_ = With_phash(Phash)

  let create ~fn = 
    trace(fun () -> Printf.sprintf "%s: start\n" __FUNCTION__);
    Phash.create ~fn ~n:10_000 |> fun p_int_map -> 
    let values = Values.create ~fn:(fn ^".values") in
    (* let deleted = Hashtbl.create 1000 in *)
    trace(fun () -> Printf.sprintf "%s: end\n" __FUNCTION__);
    { values; p_int_map; cache=Cache_2gen.create ~young_sz:1_000_000 ~old_sz:1_000_000 }

  include With_phash_  
      
  let close t = 
    Values.close t.values;
    Phash.close t.p_int_map

  (* let reload_partition t ~fn = Persistent_int_map.reload_partition t.int_map ~fn *)

  type phash = Phash.t

  let get_phash t = t.p_int_map
      
  type nonrec t = Phash.t t
end


module type S = String_string_map_intf.S

module Make_2 : S with type phash := Make_1.Phash.t
= Make_1

