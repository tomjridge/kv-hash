(** Top-level hash bucket partition, implemented using Jane St. Map


Terminology:
- partition: a collection of buckets which covers the space
- bucket: a collection (sorted and unsorted) of kvs for a particular subrange of the space

NOTE Keys are ints from 0 to Int.max_int inclusive. This means we miss out on using negative integers.

FIXME
- we could consider replacing existing kvs on each add rather than
  just placing directly into unsorted; this would give us the
  guarantee that sorted and unsorted have distinct keys; but perhaps
  we expect key updates to be rare (so, no point paying the cost of
  trying to locate in sorted)

FIXME distinguish offsets (and lengths) measured in ints {off_i} from offsets measured in bytes {off_b}

 *)

[@@@warning "-33"](* FIXME *)

open Util
open Partition_intf


(* The lowest key is 0. Every partition must contain a mapping for
   min_key. *)

(** A partition is an n-way split of the key space (ints), represented
   by a map [(k0->r0),...]. k0 is always 0. Each ri is a pointer to a
   hashbucket containing entries for keys k such that ki <= k <
   k(i+1). *)

module type S = sig
  type k
  val compare : k -> k -> int
  val min_key : k
  type r
end


module type PURE_PARTITION = sig
  type k
  type r
  type t
  val find    : t -> k -> (k * r)
  val split   : t -> k1:k -> r1:r -> k2:k -> r2:r -> t
  val to_list : t -> (k * r) list
  val of_list : (k * r) list -> t

  val write      : t -> out_channel -> unit
  val read       : in_channel -> t
end


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

  (* FIXME prefer bin_prot for persistence *)
  let write t oc = output_value oc (to_list t)

  let read ic = input_value ic |> of_list
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

  let set_split_hook t f = t.split_hook <- f

  (* FIXME prefer bin_prot for persistence *)
  let write t oc = Pure.write t.partition oc

  let read ic = 
    Pure.read ic |> fun p -> 
    { partition=p;split_hook }
end


