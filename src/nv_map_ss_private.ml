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
open Bucket_intf

(* module Cache = Cache_2gen *)
  
let hash (s:string) = 
  let h = XXHash.XXH64.hash s |> Int64.to_int in
  let h = abs h in
  assert(h>=0);
  h


(** A [string->string] map is a values file and an [int->int] map. The
   [int->int] map is really a map from hash(key) to offset(of value in
   values file). *)
type 'int_map t = {
  values: Values_file.t;
  mutable nv_int_map: 'int_map; 
  (* NOTE mutable because RO instances need to sync this from disk
     after a merge *)

  (* cache: (string,string) Cache_2gen.t; *)

  debug: (string,string) Hashtbl.t; (* All entries, for debugging only *)
}


module Make_1(S0:sig
    type raw_bucket
    module Raw_bucket:BUCKET with type t=raw_bucket
    module Nv_map_ii_:Nv_map_ii_intf.S with type raw_bucket=raw_bucket
  end) = struct
  open S0

  (** Basic implementation, no cache, no debug *)
  module Basic = struct
    let find_opt t k = 
      trace(fun () -> Printf.sprintf "%s: start\n" __FUNCTION__);
      begin
        let k' = hash k in
        Nv_map_ii_.find_opt t.nv_int_map k' |> function
        | None -> None
        | Some v' -> 
          Values_file.read_value t.values ~off:v' |> fun v -> 
          Some v
      end |> fun r -> 
      trace(fun () -> Printf.sprintf "%s: end\n" __FUNCTION__);
      r

    (* hash is the hash of k *)
    let insert_hashed t _k hash v = 
      trace(fun () -> Printf.sprintf "%s: start\n" __FUNCTION__);
      Values_file.append_value t.values v |> fun off -> 
      Nv_map_ii_.insert t.nv_int_map hash off;
      trace(fun () -> Printf.sprintf "%s: end\n" __FUNCTION__);
      ()

    let insert t k v =
      trace(fun () -> Printf.sprintf "%s: start\n" __FUNCTION__);
      insert_hashed t k (hash k) v;
      trace(fun () -> Printf.sprintf "%s: end\n" __FUNCTION__);
      ()

    (* For batch, we need the hash and insert_hashed functions so we
       can pre-sort ops by hash of key, which improves subsequent
       performance *)
    (* NOTE FIXME? we don't have to sort... it is enough to partition using
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
      trace (fun () -> Printf.sprintf "%s: end\n" __FUNCTION__);
  end

  (** To augment operations with caching and debugging, we use this signature *)
  module type S = sig
    val find_opt : Nv_map_ii_.t t -> string -> string option
    val insert_hashed : Nv_map_ii_.t t -> string -> int -> string -> unit
    val insert : Nv_map_ii_.t t -> string -> string -> unit
    val batch :
      Nv_map_ii_.t t -> (string * [`Delete | `Insert of string ]) list -> unit
  end

(*
  module With_cache(S:S) : S = struct
    open S
    let _ = Printf.printf "%s: using cached phash\n%!" __MODULE__

    let find_opt t k = 
      match Cache.find_opt' t.cache k with
      | Some (`Present v) -> (Some v)
      | Some `Absent -> None
      | None -> 
        let r = find_opt t k in
        match r with
        | None -> 
          Cache.add t.cache k `Absent;
          None
        | Some v -> 
          Cache.add t.cache k (`Present v);
          Some v          

    let insert_hashed t k hash v = 
      Cache.add t.cache k (`Present v);
      insert_hashed t k hash v

    let insert t k v = insert_hashed t k (hash k) v

    let batch t ops = 
      batch t ops;
      (* this is a good place to trim the cache... assuming we always go via batch *)
      Cache.maybe_trim t.cache ~young_sz:const_1M ~old_sz:const_1M;
      ()
  end
*)

  module With_debug(S1:S) = struct
    open S1

    let find_opt t k =
      find_opt t k |> fun r -> 
      assert(
        let expected = Hashtbl.find_opt t.debug k in
        r = expected || begin
          Printf.printf "Debug: error detected; key is %S (hash is %d); value \
                         should have been %S but was %S in %s\n%!" 
            k 
            (hash k)
            (if expected=None then "None" else Option.get expected)
            (if r=None then "None" else Option.get r)
            __MODULE__;
          (* Nv_map_ii_.show_bucket t.nv_int_map (hash k); *)
          let bucket = Nv_map_ii_.get_bucket t.nv_int_map (hash k) in
          Raw_bucket.show bucket;
          begin match Raw_bucket.find bucket (hash k) with
            | None -> ()
            | Some off -> 
              let s : string = Values_file.read_value t.values ~off in
              Printf.printf "Debug: value in values_file was: %S\n%!" s
          end;                    
          true (* continue with incorrect value FIXME *) end);
      r

    let insert_hashed t k hash v = 
      Hashtbl.replace t.debug k v;
      insert_hashed t k hash v
        
    let insert t k v =
      Hashtbl.replace t.debug k v;
      insert t k v

    (* The problem for us is that this batch operation happens in
       another thread, so the cache on the main thread does not get
       updated; so we add another operation to solely update the debug info *)
    let batch_update_debug t ops =
      ops |> List.iter (function | (k,`Insert v) -> Hashtbl.replace t.debug k v | (_k,`Delete) -> failwith "Not supported 176");
      ()

    let batch t ops =
      batch t ops;
      (* NOTE we have implemented the batch operations already; we
         just need to fix up the debug hashtbl *)
      batch_update_debug t ops;
      ()
  end
    

end



(** Putting it all together.

To create given filename fn:
- create the int->int map on fn
- create the values map on fn.values

*)
module Make_2(Raw_bucket:BUCKET) = struct

  module Raw_bucket = Raw_bucket

  module Bucket_store_ = Bucket_store.Make(Raw_bucket)

  module Nv_map_ii_ = Nv_map_ii.Make_1(Raw_bucket)(Bucket_store_)

  module Made_1 = Make_1(struct
      type raw_bucket=Raw_bucket.t
      module Raw_bucket=Raw_bucket
      module Nv_map_ii_=Nv_map_ii_
    end)

(*
  let create 
      ?buckets_fn:(buckets_fn=Config.config.bucket_store_fn)
      ?values_fn:(values_fn=Config.config.values_fn)
      () = 
    Nv_map_ii_.create_f ~buckets_fn |> fun nv_int_map -> 
    let values = Values_file.create ~fn:values_fn in
    { values; 
      nv_int_map; 
      debug=Hashtbl.create 1 }
*)

  let create values (nv_map_ii:Nv_map_ii_.t) = {
    values;
    nv_int_map=nv_map_ii;
    debug=Hashtbl.create 1 
  }    

  let batch_update_debug _t _ops = () (* FIXME for debug version, comment this line *)
  (* include Made_1.With_debug(Made_1.Basic)  *)
  (* FIXME add cache when sure this is working correctly *)
  include Made_1.Basic


  (** NOTE close will close the underling values file, and the nv_map_ii *)
  let close t = 
    Values_file.close t.values;
    Nv_map_ii_.close t.nv_int_map

  type nv_map_ii = Nv_map_ii_.t

  let get_nv_map_ii t : nv_map_ii = t.nv_int_map
      
  type nonrec t = Nv_map_ii_.t t

  let get_values_file t = t.values
end

(** NOTE prefer Make_2 since it provides the Nv_map_ii_ functions *)
module Make_3(Raw_bucket:BUCKET) : Nv_map_ss_intf.S = Make_2(Raw_bucket)


module Nv_map_ss0 = Make_2(Bucket.Bucket0)

(*
(** Add an extra function we need; expose Nv_map_ii_ submodule *)
module type S2 = sig

  module Nv_map_ii_ : module type of Make_2.Nv_map_ii_

  include Nv_map_ss_intf.S with type nv_map_ii := Nv_map_ii_.t

  val batch_update_debug : t -> Nv_map_ss_intf.op list -> unit
end 


module Make_3 : S2 = Make_2
*)
