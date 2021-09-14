(** Top-level hash bucket partition, implemented using Jane St. Map


Terminology:
- partition: a collection of buckets which covers the space
- bucket: a collection (sorted and unsorted) of kvs for a particular subrange of the space

FIXME
- we could consider replacing existing kvs on each add rather than
  just placing directly into unsorted; this would give us the
  guarantee that sorted and unsorted have distinct keys; but perhaps
  we expect key updates to be rare (so, no point paying the cost of
  trying to locate in sorted)


 *)

[@@@warning "-33"](* FIXME *)

open Util
open Partition_intf


(* We could just use the range 0 to Int.max_int; this would make
   things easier, although we miss out on 2 bits per key

In this case, we don't need None, and the lowest key is 0. Every
   partition must contain a mapping for min_key.

 *)


module Make_partition(S:sig
    type k
    val compare : k -> k -> int
    val min_key : k
    type r
  end) : PARTITION with type k=S.k and type r=S.r = struct
  include S

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
  type t = partition

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
    | Some (k1,r) -> (k1,r)

  (* split a partition (k1,_) into (k1,r1) (k2,r2) *)
  let split (p:partition) ~k1 ~r1 ~k2 ~r2 =
    assert(Map.mem p k1);
    assert(not (Map.mem p k2));
    Map.change p k1 ~f:(function | None -> failwith "impossible" | Some _r1 -> Some r1) |> fun p -> 
    Map.set p ~key:k2 ~data:r2 |> fun p -> 
    p

  (* let ops = { find; split; of_list; to_list } *)
end

module Config = struct

  let blk_sz = 4096

  let ints_per_block = 4096 / Bigarray.kind_size_in_bytes Bigarray.int

  let int_offset ~blk = blk * ints_per_block

end
open Config

module Interpolate_ = Interpolate(struct type k = int type v = int end)
          
(* A bucket is stored at (off,len) within the larger data store;
   FIXME could just return bucket_data? do we need off and len? *)
type bucket = {
  off  : int;
  len  : int;
  bucket_data : int_array
}


module Bucket = struct

  (* NOTE a partition should be a multiple of the block size
     (measured in "kind_size_in_bytes int"); actually, for
     crash-resilience a partition should be block aligned and block
     sized, and the code should work independent of the order of
     page flushes *)

  let bucket_size = Config.ints_per_block

  (* the sorted array starts at position 0, with the length of the
     elts, and then the sorted elts, and then 0 bytes, then the
     unsorted length, unsorted elts, and 0 bytes *)

  let max_unsorted = 10 (* say; this is the number of keys; we
                           also store the values as well *)

  (* subtract 2 for the length fields: len_sorted, len_unsorted *)
  let max_sorted = (bucket_size - (2*max_unsorted) - 2) / 2

  let _ = assert(max_sorted >= 400)

  (** Positions/offsets within the buffer that we store certain info *)
  module Ptr = struct
    let len_sorted = 0
    let sorted_start = 1
    let len_unsorted = 2*max_sorted +1
    let unsorted_start = 2*max_sorted +2
  end

  (* for debugging *)
  type exported_bucket = {
    sorted: (int*int) list;
    unsorted: (int*int) list
  }

  module With_bucket(B:sig
      val bucket : bucket
    end) = struct
    open B

    (* abbrev; we used to refer to the bucket as a partition; but
       partition can mean either "partitioning" or "single
       partition" *)
    let p = bucket

    (* shorter abbreviation *)
    let arr = bucket.bucket_data


    (* in kv pairs *)
    let len_sorted () = arr.{ Ptr.len_sorted }

    let set_len_sorted n = arr.{ Ptr.len_sorted } <- n

    let sorted = Bigarray.Array1.sub arr Ptr.sorted_start (2*max_sorted)

    let len_unsorted () = arr.{ Ptr.len_unsorted }

    let set_len_unsorted n = arr.{ Ptr.len_unsorted } <- n

    let unsorted = Bigarray.Array1.sub arr Ptr.unsorted_start (2*max_unsorted)

    (* unsorted *)
    module U = struct
      let ks i = unsorted.{ 2*i }[@@inline]
      let vs i = unsorted.{ 2*i +1 }[@@inline]
    end

    (* sorted *)
    module S = struct
      let ks i = sorted.{ 2*i }[@@inline]
      let vs i = sorted.{ 2*i +1 }[@@inline]
    end

    let find_sorted k = 
      Interpolate_.find 
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
      (* FIXME assert dst is within bucket_data *)
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

    (* FIXME maybe cleaner to do the split, then add; so add could
       return `Needs_split *)

    (** If there are less than len_unsorted free entries in sorted,
        we can't merge; in this case, we need to create 2 new
        partitions, sort the entries and split into half for each
        partition, then return with the new partitions (and note that
        we need to GC/recycle the old partition at some point) *)

    (** The kv parameter is an extra key-value that we need to add
        to the split. NOTE alloc returns a new *clean* bucket
        (lengths are 0 etc) *)
    let split_with_addition ~alloc_bucket kv = 
      let p1,p2 = alloc_bucket(),alloc_bucket() in
      (* check clean partitions *)
      assert(p1.bucket_data.{ Ptr.len_sorted } = 0
             && p1.bucket_data.{ Ptr.len_unsorted } = 0);
      assert(p2.bucket_data.{ Ptr.len_sorted } = 0
             && p2.bucket_data.{ Ptr.len_unsorted } = 0);
      assert(p1.len = p.len && p2.len = p.len);
      let len1 = len_sorted () in
      let len2 = len_unsorted () in
      assert(len1 + len2 <= max_sorted);
      (* copy unsorted into an array (including kv), and sort *)
      let kvs2 = Array.init (len2+1) (fun i -> if i < len2 then U.ks i, U.vs i else kv) in
      Array.sort (fun (k1,_) (k2,_) -> Int.compare k1 k2) kvs2;
      (* now merge directly into p1.sorted and p2.sorted; roughly
         half in p1, half in p2 (p1 may duplicate some old entries
         that get overridden by entries in p2) *)
      let cut_point = (len1+len2+1) / 2 in
      let ks1 i = sorted.{ 2*i } in
      let vs1 i = sorted.{ 2*i +1 } in
      let ks2 i = kvs2.(i) |> fst in
      let vs2 i = kvs2.(i) |> snd in
      let count = ref 0 in
      let set i k v = 
        match !count < cut_point with
        | true -> 
          (* fill p1 *)
          p1.bucket_data.{ Ptr.sorted_start + 2*i } <- k;
          p1.bucket_data.{ Ptr.sorted_start + 2*i +1} <- v;
          incr count;
          ()
        | false -> 
          (* fill p2 *)
          let i = i - cut_point in
          p2.bucket_data.{ Ptr.sorted_start + 2*i } <- k;
          p2.bucket_data.{ Ptr.sorted_start + 2*i +1} <- v;
          (* no need to incr count *)
          ()
      in
      merge 
        ~ks1 ~vs1 ~len1
        ~ks2 ~vs2 ~len2
        ~set
        () |> fun n -> 
      p1.bucket_data.{ Ptr.len_sorted } <- cut_point;
      assert(len1 + len2 < 2*len1);
      assert(n-cut_point>0); 
      (* FIXME are we sure this is the case? yes, if cut_point <
         original sorted length; why is this the case? need l1+l2 <
         2*l1, which should be the case if l2 small compared to l1
      *)
      p2.bucket_data.{ Ptr.len_sorted } <- n - cut_point;
      (* get lowest key in p2 *)
      let k2 = p2.bucket_data.{ Ptr.sorted_start } in
      (p1,k2,p2)

    let add ~alloc_bucket k v = 
      (* try to add in unsorted *)
      let len2 = len_unsorted () in
      match len2 < max_sorted with
      | true -> (
          unsorted.{ 2*len2 } <- k;
          unsorted.{ 2*len2 +1 } <- v;
          set_len_unsorted (len2+1);
          `Ok)
      | false -> 
        (* check if we can merge existing unsorted elts *)
        let len1 = len_sorted () in
        match len1+len2 <= max_sorted with
        | true -> 
          (* so merge, and add a new unsorted elt *)
          merge_unsorted ();
          unsorted.{ 0 } <- k;
          unsorted.{ 1 } <- v;
          set_len_unsorted 1;
          `Ok
        | false -> 
          (* unsorted is full, and not enough space to merge, so we need to split *)
          split_with_addition ~alloc_bucket (k,v) |> fun (p1,k,p2) -> 
          `Split(p1,k,p2)

    let export () = 
      let len1 = len_sorted () in
      let len2 = len_unsorted () in
      (* copy sorted into an array *)
      let kvs1 = Array.init len1 (fun i -> S.ks i, S.vs i) in
      (* copy unsorted into an array, and sort *)
      let kvs2 = Array.init len2 (fun i -> U.ks i, U.vs i) in
      { sorted=Array.to_list kvs1; unsorted=Array.to_list kvs2 }      

    (* FIXME what about delete? *)
  end (* With_bucket *)

  let add_to_bucket ~bucket = 
    let open With_bucket(struct let bucket=bucket end) in
    add

  let _ : bucket:bucket ->
alloc_bucket:(unit -> bucket) ->
int -> int -> [ `Ok | `Split of bucket * int * bucket ] = add_to_bucket

  let find ~bucket = 
    let open With_bucket(struct let bucket=bucket end) in
    find

  let _ : bucket:bucket -> int -> int option = find

  let export bucket = 
    let open With_bucket(struct let bucket=bucket end) in
    export()    

end (* Bucket *)


type export_t = {
  partition: (int*int) list;
  buckets: Bucket.exported_bucket list
}

module Make() = struct

  module S = struct
    type k = int
    let compare = Int.compare
    let min_key = Int.zero
    type r = int  (* r is the block offset within the data file, measured in blocks *)
  end

  module Partition = Make_partition(S)
  module Prt = Partition      

  (** Create an initial n-way partition, with values provided by alloc *)
  let initial_partitioning ~alloc ~n = 
    let delta = Int.max_int / n in
    Base.List.range ~stride:delta ~start:`inclusive ~stop:`inclusive 0 ((n-1)*delta) |> fun ks -> 
    ks |> List.map (fun x -> x,alloc ()) |> fun krs -> 
    Prt.of_list krs

  (* Runtime handle *)
  type t = {
    fn                : string; (* filename *)
    fd                : Unix.file_descr;
    data              : (int,int_elt) Mmap.t; (* all the data in the file *)
    alloc_counter     : int ref;
    alloc             : unit -> int;
    mutable partition : Prt.t;
    (* add_to_bucket     : bucket -> int -> int -> [ `Ok | `Split of bucket * int * bucket ]; *)
  }

  (** n is the initial number of partitions of 0...max_int *)
  let create ~fn ~n =
    Unix.(openfile fn [O_CREAT;O_RDWR; O_TRUNC] 0o640) |> fun fd -> 
    let data = Mmap.of_fd fd Bigarray.int in
    (* keep block 0 for header etc *)
    let alloc_counter = ref 1 in
    let alloc () = 
      !alloc_counter |> fun r -> 
      incr alloc_counter;
      r
    in
    let partition = initial_partitioning ~alloc ~n in
    { fn; fd; data; alloc_counter; alloc; partition }

  let alloc_bucket t = 
    t.alloc () |> fun i -> 
    let off = Config.int_offset ~blk:i in
    let len = Config.ints_per_block in
    { off; len; bucket_data=Mmap.sub t.data ~off ~len }

  let add_to_bucket ~t ~bucket k v = 
    Bucket.add_to_bucket ~alloc_bucket:(fun () -> alloc_bucket t) ~bucket k v

  let find_bucket ~partition ~data k = 
    Prt.find k partition |> fun (k,r) -> 
    (* r is the partition offset within the store *)
    let off = r * Bucket.bucket_size in
    let len = Bucket.bucket_size in
    let bucket_data = Mmap.sub data ~off ~len in
    k,{ off; len; bucket_data }

  
  (* public interface: insert, find (FIXME delete) *)
    
  let insert t k v = 
    (* find bucket *)
    let k1,bucket = find_bucket ~partition:t.partition ~data:t.data k in
    add_to_bucket ~t ~bucket k v |> function
    | `Ok -> ()
    | `Split(b1,k2,b2) -> 
      Printf.printf "Split partition %d into %d %d\n%!" k1 k1 k2;
      t.partition <- Prt.split t.partition ~k1 ~r1:b1.off ~k2 ~r2:b2.off;
      ()  
    
  let find t k = 
    let _,bucket = find_bucket ~partition:t.partition ~data:t.data k in
    Bucket.find ~bucket k

  let export t = 
    let { partition; data; _ } = t in
    Prt.to_list partition |> fun krs -> 
    krs |> List.map (fun (k,_) -> find_bucket ~partition ~data k |> function (_,b) -> Bucket.export b) |> fun buckets -> 
    {partition=krs;buckets}

  let _ : t -> export_t = export

end (* Make *)


