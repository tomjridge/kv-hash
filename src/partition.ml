(** Top-level hash bucket partition, implemented using Jane St. Map


Terminology:
- partitioning: a collection of partitions which covers the space
- partition: a single partition, covering part of the space

 *)

[@@@warning "-33"](* FIXME *)

open Util
open Partition_intf


(* We could just use the range 0 to Int.max_int; this would make
   things easier, although we miss out on 2 bits per key

In this case, we don't need None, and the lowest key is 0. Every
   partition must contain a mapping for min_key.

 *)

let low = Int.zero

let high = Int.max_int


module Make(S:sig
    type k
    val compare : k -> k -> int
    val min_key : k
    type r
  end) = struct
  open S

  module K = struct 
    type t = k 
    let compare = compare
    let sexp_of_t: t -> Base.Sexp.t = fun _ -> Base.Sexp.Atom __LOC__
    (** ASSUMES this function is never called in our usecases; FIXME
        it is called; how? *)
  end

  module C = struct
    type t = K.t
    include Base.Comparator.Make(K)
  end

  let comparator : _ Base.Map.comparator = (module C)

  module Map = Base.Map

  type partition = (k,r,C.comparator_witness) Map.t 

  (* this primed version works with k options *)
  let to_list b : (k * r) list = b |> Map.to_alist 

  let of_list krs = 
    krs |> Map.of_alist_exn comparator |> fun p -> 
    assert(Map.mem p min_key);
    p

  let find k b : k * r = 
    (* find the greatest key <= k *)
    Map.closest_key b `Less_or_equal_to k |> function
    | None -> failwith "find: impossible: min_key must be a member"
    | Some (k1,r) -> 
      (k1,r)

  (* split a partition (k1,_) into (k1,r1) (k2,r2) *)
  let split (p:partition) ~k1 ~r1 ~k2 ~r2 =
    assert(Map.mem p k1);
    assert(not (Map.mem p k2));
    Map.change p k1 ~f:(function | None -> failwith "impossible" | Some _r1 -> Some r1) |> fun p -> 
    Map.set p ~key:k2 ~data:r2 |> fun p -> 
    p

  let ops = { find; split; of_list; to_list }

end

module Config = struct

  let blk_sz = 4096

  let ints_per_block = 4096 / Bigarray.kind_size_in_bytes Bigarray.int

  let int_offset ~blk = blk * ints_per_block

end
open Config

(** Interpolate once, then scan. ASSUMES we are using Stdlib.( < > =) etc *)
module Interpolate(S:sig
    type k = int
    type v
  end) 
= struct
  open S

  let find ~(len:int) ~(ks:int->k) ~(vs:int->v) k =
    let low_k = ks 0 in
    let high_k = ks (len -1) in
    match () with 
    | _ when k < low_k -> None
    | _ when k > high_k -> None
    | _ when k = low_k -> Some (vs 0)
    | _ when k = high_k -> Some (vs (len -1))
    | _ -> 
      (* interpolate between low_k and high_k *)
      let high_low = float_of_int (high_k - low_k) in
      (* delta is between 0 and 1 *)
      let delta = float_of_int (k - low_k) /. high_low in
      let i = int_of_float (delta *. (float_of_int len)) in              
      (* clip to len-1 *)
      let i = min (len - 1) i in
      match k = ks i with
      | true -> Some (vs i)
      | false -> 
        begin
          let dir = if k < ks i then -1 else +1 in
          (* scan from i+dir, in steps of dir, until we know we can't find k *)
          i+dir |> iter_k (fun ~k:kont j -> 
              (* we can forget about the first and last positions... *)
              match j >= len-1 || j<= 0 with
              | true -> None
              | false -> 
                let kj = ks j in
                match k=kj with
                | true -> Some (vs j)
                | false -> 
                  match (k > kj && dir = -1) || (k < kj && dir = 1) with
                  | true -> None
                  | false -> kont (j+dir))
        end

  let _ : len:k -> ks:(k -> k) -> vs:(k -> v) -> k -> v option = find
end

(* Merge two sorted sequences; ks2 take precedence; perhaps it would
   be cleaner to actually use sequences *)
module Merge(S:sig
    type k
    type v
    val ks1 : int -> k
    val vs1 : int -> v
    val len1 : int
    val ks2 : int -> k
    val vs2 : int -> v
    val len2: int
    val set : int -> k -> v -> unit
  end) = struct
  open S

  let merge_rest ks vs j i = 
    (j,i) |> iter_k (fun ~k:kont (j,i) -> 
        match j >= len1 with 
        | true -> i (* return the length of the merged result *)
        | false -> 
          set i (ks j) (vs j);
          kont (j+1,i+1))

  let merge () = 
    (0,0,0) |> iter_k (fun ~k:kont (i1,i2,i) -> 
        match () with
        | _ when i1 >= len1 -> merge_rest ks2 vs2 i2 i
        | _ when i2 >= len2 -> merge_rest ks1 vs1 i1 i
        | _ -> 
          let k1,k2 = ks1 i1, ks2 i2 in
          match k1 < k2 with 
          | true -> 
            set i k1 (vs1 i1);
            kont (i1+1,i2,i+1)
          | false -> 
            match k1 = k2 with
            | true -> 
              set i k2 (vs2 i2);
              (* NOTE we drop from ks1 as well *)
              kont (i1+1,i2+1,i+1)
            | false -> 
              assert(k2 < k1);
              set i k2 (vs2 i2);
              kont (i1,i2+1,i+1))             
end

let merge (type k v) ~ks1 ~vs1 ~len1 ~ks2 ~vs2 ~len2 ~set = 
  let module Merge = Merge(struct 
      type nonrec k=k 
      type nonrec v=v 
      let ks1,vs1,len1,ks2,vs2,len2,set = ks1,vs1,len1,ks2,vs2,len2,set
    end) 
  in
  Merge.merge



module Int = struct

  module S = struct
    type k = int
    let compare = Int.compare
    let min_key = Int.zero
    type r = int  (* r is the block offset within the data file, measured in blocks *)
  end

  include Make(S)

(*
  let k_lt k1 k2 = compare k1 k2 < 0
  let k_gt k1 k2 = compare k1 k2 > 0
  let k_le k1 k2 = compare k1 k2 <= 0
  let k_eq k1 k2 = compare k1 k2 = 0
*)

  module At_runtime(R:sig 
      val data: (int,int_elt) Mmap.t
    end) = struct

    (** Create an initial n-way partition, with values provided by alloc *)
    let initial_partitioning' ~alloc ~n = 
      let delta = Int.max_int / n in
      Base.List.range ~stride:delta ~start:`inclusive ~stop:`inclusive 0 ((n-1)*delta) |> fun ks -> 
      ks |> List.map (fun x -> x,alloc ()) |> fun krs -> 
      of_list krs


    (* keep block 0 for header etc *)
    let alloc = 
      let counter = ref 1 in
      fun () -> 
        !counter |> fun r -> 
        incr counter;
        r

    let initial_partitioning ~n = initial_partitioning' ~alloc ~n    


    (* NOTE we should talk in terms of partitions, but a partition
       should be a multiple of the block size (measured in
       "kind_size_in_bytes int") *)

    let partition_size = Config.ints_per_block

    (* A partition is a range within the larger data store *)
    type partition = {
      off  : int;
      len  : int;
      part_data : int_array
    }

    let find_partition k t = 
      find k t |> fun (_,r) -> 
      (* r is the partition offset within the store *)
      let off = r * partition_size in
      let len = partition_size in
      let part_data = Mmap.sub R.data ~off ~len in
      { off; len; part_data }

    (* here we put a bit more structure on the partition; we have some
       initial header info *)

    module With_partition(P:sig
        val p : partition
      end) = struct
      open P

      (* the sorted array starts at position 0, with the length of the
         elts, and then the sorted elts, and then 0 bytes, then the
         unsorted length, unsorted elts, and 0 bytes *)

      let max_unsorted = 10 (* say; this is the number of keys; we
                               also store the values as well *)

      (* subtract 2 for the length fields: len_sorted, len_unsorted *)
      let max_sorted = (partition_size - (2*max_unsorted) - 2) / 2

      let _ = assert(max_sorted >= 400)

      (* shorter abbreviation *)
      let arr = p.part_data

      module Ptr = struct
        let len_sorted = 0
        let sorted_start = 1
        let len_unsorted = 2*max_sorted +1
        let unsorted_start = 2*max_sorted +2
      end

      (* in kv pairs *)
      let len_sorted () = arr.{ Ptr.len_sorted }

      let set_len_sorted n = arr.{ Ptr.len_sorted } <- n

      let sorted = Bigarray.Array1.sub arr Ptr.sorted_start (2*max_sorted)
      
      let len_unsorted () = arr.{ Ptr.len_unsorted }

      let set_len_unsorted n = arr.{ Ptr.len_unsorted } <- n

      let unsorted = Bigarray.Array1.sub arr Ptr.unsorted_start (max_unsorted * 2)

      module U = struct
        let ks i = unsorted.{ 2*i }[@@inline]
        let vs i = unsorted.{ 2*i +1 }[@@inline]
      end

      (*
      (* Sorted *)
      module S = struct
        let ks i = sorted.{ 2*i }[@@inline]
        let vs i = sorted.{ 2*i +1 }[@@inline]
      end
      *)

      (** NOTE assumes there is space to add, ie len_unsorted < max_unsorted *)
      let add_unsorted k v = 
        let x = len_unsorted () in
        assert(x < max_unsorted);
        unsorted.{ 2*x } <- k;
        unsorted.{ 2*x +1 } <- v;
        set_len_unsorted (x+1);
        ()

      module Interpolate = Interpolate(struct type k = int type v = int end)
          
      let find_sorted k = 
        Interpolate.find 
          ~len:(len_sorted())
          ~ks:(fun i -> sorted.{2*i})
          ~vs:(fun i -> sorted.{2*i +1})
          k

      (** Just scan through unsorted *)
      let find_unsorted k = 
        let len = len_sorted () in
        0 |> iter_k (fun ~k:kont i -> 
            match i >= len with
            | true -> None
            | false -> 
              match U.ks i = k with
              | true -> Some (U.vs i)
              | false -> kont (i+1))

      (* new items are placed in unsorted, so we look there first *)
      let find k = find_unsorted k |> function
        | None -> find_sorted k
        | Some v -> Some v

      (** merge unsorted into sorted; NOTE assumes there is enough space to insert *)
      let merge_unsorted () = 
        let len1 = len_sorted () in
        let len2 = len_unsorted () in
        assert(len1 + len2 <= max_sorted);
        (* copy unsorted into an array, and sort *)
        let kvs2 = Array.init len2 (fun i -> U.ks i, U.vs i) in
        Array.sort (fun (k1,_) (k2,_) -> Int.compare k1 k2) kvs2;
        (* move sorted so that the elts reside at the end of the
           partition *)
        (* FIXME assert dst is within part_data *)
        let dst = Bigarray.Array1.sub arr (Ptr.sorted_start+2*max_unsorted) (2*len1) in
        Bigarray.Array1.blit sorted dst;
        (* now merge sorted with kvs2, placing elts back into sorted *)
        let ks1 i = dst.{ 2*i } in
        let vs1 i = dst.{ 2*i +1 } in
        let ks2 i = kvs2.(i) |> fst in
        let vs2 i = kvs2.(i) |> snd in
        merge 
          ~ks1 ~vs1 ~len1
          ~ks2 ~vs2 ~len2
          ~set:(fun i k v -> sorted.{2*i}<- k; sorted.{2*i +1} <- v; ())
          () |> fun n -> 
        set_len_sorted n;
        set_len_unsorted 0;
        ()
               
    end    


  end
    
end


