(** Top-level bucket partition, implemented using Jane St. Map

Given that we want to store (key,value) pairs, a partition is a way to
   map a key to a particular bucket, where we store the corresponding
   (key,value).

We assume that the keys are roughly uniformly distributed. (In our use
   case, they are hashes generated by some "good" hash function.)

We assume the key space is a linear order (in fact, our usecase has
   keys as ints... actually hashes). A partition is then (finitely
   represented) total map from key to bucket.

How do we implement this? Each bucket covers a range l_i <= _ <
   h_i. The ranges are mutually disjoint and cover the key space. So,
   h_i = l_{i+1} say. The partition contains a map from each l_i to a
   bucket identifier. Given a particular key k, we find the l_i which
   is just <= k (i.e., it is <= k, and is the largest such l) and look
   up the appropriate bucket identifier. This can be done efficiently
   using a map based on binary search trees. The standard library
   doesn't support this operation, but Jane St. map library does.


The lowest key (min_key) is 0 in our use case. Every partition
   explicitly contains at least a mapping for min_key.

Sometimes a bucket becomes full. In this case, we split the bucket
   into two new buckets (half the kvs in one, half in another), and
   split the old range l_i <= _ < h_i into two new ranges,
   corresponding to the two new buckets.


Terminology:

- partition: a total map from key to bucket identifier; each bucket
   corresponds to a subrange of the keyspace; these ranges are
   disjoint and cover the keyspace

- bucket: a collection of kvs for a particular subrange of the space

NOTE In our use case keys are ints from 0 to Int.max_int
   inclusive. This means we miss out on using negative integers.

FIXME distinguish offsets (and lengths) measured in ints {off_i} from
   offsets measured in bytes {off_b}
 *)

[@@@warning "-33"](* FIXME *)

open Util
open Partition_intf


module type S = sig
  type k
  val compare : k -> k -> int
  val min_key : k
  type r  (** bucket identifiers *)
end

(** Pure partitions *)
module Make_1(S:S) : PURE_PARTITION with type k=S.k and type r=S.r 
= struct
  include S

  (* Construct a map, using [compare]; we use Base.Map because it
     supports extra operations (eg closest_key) which we need. *)
  module Comparator_ = Make_comparator(S)

  type comparator_witness = Comparator_.comparator_witness
  let comparator : (k,comparator_witness) Base.Map.comparator = Comparator_.comparator

  module Map = Base.Map


  (* A partition is just a map from k to r *)
  type partition = (k,r,comparator_witness) Map.t 
  type t = partition

  let to_list b : (k * r) list = b |> Map.to_alist 
                                 
  let of_list krs = 
    krs |> Map.of_alist_exn comparator |> fun p -> 
    assert(Map.mem p min_key);
    p

  let find t k : k * r = 
    (* find the greatest key <= k *)
    Map.closest_key t `Less_or_equal_to k |> function
    | None -> failwith "find: impossible: min_key must be a member"
    | Some (k1,r) -> (k1,r)

  (* split a partition (k1,_) into (k1,r1) (k2,r2) *)
  let split t ~k1 ~r1 ~k2 ~r2 =
    let p = t in
    assert(Map.mem p k1);
    assert(not (Map.mem p k2));
    let p = Map.change p k1 
        ~f:(function | None -> failwith "impossible" 
                     | Some _r1 -> Some r1)
    in
    Map.set p ~key:k2 ~data:r2 |> fun p -> 
    p

  let length t = Map.length t

(*
  (* FIXME prefer bin_prot for persistence *)
  let write t oc = output_value oc (to_list t)

  let read ic = input_value ic |> of_list
*)
      
end


(** Mutable partitions, with split hook *)
module Make_2(S:S) : PARTITION with type k=S.k and type r=S.r 
= struct
  include S

  module Pure = Make_1(S)

  (* default *)
  let split_hook = fun () -> ()

  type t = {
    mutable partition:Pure.t;
    mutable split_hook:unit->unit;
  }

  let to_list t : (k * r) list = t.partition |> Pure.to_list
                                 
  let of_list krs = 
    Pure.of_list krs |> fun p -> 
    {partition=p;split_hook}

  let find t k : k * r = Pure.find t.partition k

  (* split a partition (k1,_) into (k1,r1) (k2,r2) *)
  let split t ~k1 ~r1 ~k2 ~r2 =
    Pure.split t.partition ~k1 ~r1 ~k2 ~r2 |> fun p -> 
    t.partition <- p;
    t.split_hook ();
    ()

  (* let set_split_hook t f = t.split_hook <- f *)

  let length t = Pure.length t.partition
(*
  (* FIXME prefer bin_prot for persistence *)
  let write t oc = Pure.write t.partition oc

  let read ic = 
    Pure.read ic |> fun p -> 
    { partition=p;split_hook }
*)
end

module Make_partition = Make_2


(** Default partition instance, mutable, k=int, r=int *)
module Partition_ii = struct

  module S = struct
    type k = int
    let compare = Int.compare
    let min_key = Int.zero
    type r = int  (* r is the block offset within the data file, measured in blocks *)
  end

  module Partition = Make_partition(S)

  include Partition


  (** Some I/O functions *)

  (* NOTE there were some segfaults using output_value on the map
     itself, so here we take a more cautious approach: we use
     output_value for small items, and we ensure tail recursion *)
  let write_fn t ~fn =
    let oc = Stdlib.open_out_bin fn in
    let kvs = t |> to_list in
    begin
      kvs |> iter_k (fun ~k:kont kvs -> 
          match kvs with
          | [] -> ()
          | (k,v)::kvs -> 
            Stdlib.output_value oc (k,v);
            kont kvs)
    end |> fun () -> 
    Stdlib.close_out_noerr oc;
    ()

  let read_fn ~fn =
    let ic = Stdlib.open_in_bin fn in
    begin
      ([]:(int*int)list) |> iter_k (fun ~k:kont xs -> 
          (try 
             Stdlib.input_value ic |> fun (k,v) -> 
             Some (k,v)
           with End_of_file -> None) |> function
          | None -> xs
          | Some (k,v) -> kont ((k,v)::xs))
    end |> fun kvs -> 
    Stdlib.close_in_noerr ic;
    of_list kvs            

end
