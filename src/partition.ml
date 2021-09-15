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


module Make_partition(S:S) 
  : PARTITION with type k=S.k and type r=S.r 
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
    let p = Map.change p k1 
        ~f:(function | None -> failwith "impossible" 
                     | Some _r1 -> Some r1)
    in
    Map.set p ~key:k2 ~data:r2 |> fun p -> 
    p

  (* FIXME prefer non-marshalling for persistence *)
  let write t oc =
    output_value oc (to_list t)

  let read ic = input_value ic |> of_list
end


module Interpolate_ = Interpolate(struct type k = int type v = int end)
          
(* A bucket is stored at (off,len) within the larger data store;
   FIXME could just return bucket_data? do we need off and len? *)
type bucket = {
  off  : int;
  len  : int;
  bucket_data : int_array
}

include struct
  (* for debugging *)
  open Sexplib.Std
  type exported_bucket = {
    sorted: (int*int) list;
    unsorted: (int*int) list
  }[@@deriving sexp]
end


module type BUCKET_CONFIG = sig
  val max_sorted: int  
  val max_unsorted: int
end


module Bucket(C:BUCKET_CONFIG) = struct


  (* NOTE for crash-resilience a partition should be block aligned and
     block sized, and the code should work independent of the order of
     page flushes *)

  (* NOTE sorted should always have unique keys; unsorted may not have
     unique keys *)

  (* let bucket_size = Config.ints_per_block *)

  (* the sorted array starts at position 0, with the length of the
     elts, and then the sorted elts, and then 0 bytes, then the
     unsorted length, unsorted elts, and 0 bytes *)

  open C 

  let bucket_size_ints = 1 + 2*max_sorted + 1 + 2*max_unsorted

  let bucket_size_bytes = bucket_size_ints * Bigarray.kind_size_in_bytes Bigarray.int

  let _ = assert(max_sorted >= max_unsorted)

  (** Positions/offsets within the buffer that we store certain info *)
  module Ptr = struct
    let len_sorted = 0
    let sorted_start = 1
    let len_unsorted = 2*max_sorted +1 (* position at which we store the len of unsorted *)
    let unsorted_start = len_unsorted +1
  end

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

    (* FIXME could avoid sorted and unsorted subarrays, and just index in U and S *)
    let sorted = Bigarray.Array1.sub arr Ptr.sorted_start (2*max_sorted)

    let len_unsorted () = arr.{ Ptr.len_unsorted }

    let set_len_unsorted n = arr.{ Ptr.len_unsorted } <- n

    let unsorted = 
      assert( 
        let len = Bigarray.Array1.dim arr in
        Ptr.unsorted_start + (2*max_unsorted) <= len
        || begin
          Printf.printf "%d %d %d\n%!" Ptr.unsorted_start (2*max_unsorted) len;
          false end);
      Bigarray.Array1.sub arr Ptr.unsorted_start (2*max_unsorted)

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
        ~ks:S.ks
        ~vs:S.vs
        k

    (** Just scan through unsorted, newest elts first *)
    let find_unsorted k = 
      let len = len_sorted () in
      len -1 |> iter_k (fun ~k:kont i -> 
          match i < 0 with
          | true -> None
          | false -> 
            match U.ks i = k with
            | true -> Some (U.vs i)
            | false -> kont (i-1))

    (* new items are placed in unsorted, so we look there first *)
    let find k = find_unsorted k |> function
      | None -> find_sorted k
      | Some v -> Some v

    (* NOTE need to remove duplicates *)

    module Map = Map.Make(Int)

    (* convert to sorted array; include some additional elts xs *)
    let unsorted_to_sorted xs = 
      let len = len_unsorted () in
      let m = Map.of_seq (List.to_seq xs) in
      (0,m) |> iter_k (fun ~k:kont (i,m) -> 
          match i < len with
          | false -> m
          | true -> 
            Map.add (U.ks i) (U.vs i) m |> fun m -> 
            kont (i+1,m)) |> fun m -> 
      Map.to_seq m |> fun seq -> 
      Array.of_seq seq     

    (** merge unsorted into sorted; NOTE assumes there is enough space to insert *)
    let merge_unsorted () = 
      trace(fun () -> "merge_unsorted");
      let len1 = len_sorted () in
      let len2 = len_unsorted () in
      (* FIXME the following is too harsh, since we may be deleting
         some elements; one thing we can be sure is that a temporary
         buffer of len max_sorted+max_unsorted can hold all possible
         merged results; then we can split if the result is larger
         than max_sorted *)
      assert(len1 + len2 <= max_sorted);
      (* copy unsorted into an array, and sort *)
      let kvs2 = unsorted_to_sorted [] in
      let len2 = Array.length kvs2 in
      (* move sorted so that the elts reside at the end of the
         partition *)
      (* FIXME assert dst is within bucket_data *)
      let dst = Bigarray.Array1.sub arr (Ptr.sorted_start+2*max_unsorted) (2*len1) in
      (* FIXME perhaps use a tmp buffer? what about concurrent read?
         yes, need to use a tmp buffer; this also helps with knowing
         the length of the result *)
      Bigarray.Array1.blit (Bigarray.Array1.sub sorted 0 (2*len1)) dst;
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
      assert(not (len1 + len2 <= max_sorted));
      (* copy unsorted into an array (including kv), and sort *)
      let kvs2 = unsorted_to_sorted [kv] in
      (* FIXME may need another len2 here, in case of duplicates, and
         we also added a new elt *)
      let len2 = Array.length kvs2 in
      (* now merge directly into p1.sorted and p2.sorted; roughly
         half in p1, half in p2 (p1 may duplicate some old entries
         that get overridden by entries in p2) *)
      let cut_point = (len1+len2+1) / 2 in
      let ks1 i = S.ks i in
      let vs1 i = S.vs i in
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
      assert(len1 + len2 <= 2*len1); (* FIXME logic *)
      assert(
        n-cut_point>0 || begin
          Printf.printf "%d %d %d %d\n%!" len1 len2 cut_point n;
          false
        end
      ); 
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
      match len2 < max_unsorted with
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
  buckets: exported_bucket list
}

module type CONFIG = sig
  include BUCKET_CONFIG
  val blk_sz : int (* in bytes *)
end

module Make(Config:CONFIG) = struct

  open Config

  module Bucket = Bucket(Config)

  let blk_sz = Config.blk_sz

  let ints_per_block = blk_sz / Bigarray.kind_size_in_bytes Bigarray.int

  let _ = assert(Bucket.bucket_size_bytes <= blk_sz)

  let _ = 
    let open Sexplib.Std in
    Sexplib.Sexp.to_string_hum 
      [%message "Config"
        ~blk_sz:(blk_sz : int)
        ~ints_per_block:(ints_per_block : int)
        ~max_sorted:(max_sorted : int)
        ~max_unsorted:(max_unsorted : int)
        ~bucket_size_ints:(Bucket.bucket_size_ints : int)
        ~bucket_size_bytes:(Bucket.bucket_size_bytes : int)
      ]
    |> print_endline


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

  let const_4GB = 4_294967296

  (* create with an initial partition *)
  let create_p ~fn ~partition = 
    Unix.(openfile fn [O_CREAT;O_RDWR; O_TRUNC] 0o640) |> fun fd -> 
    Unix.truncate fn (const_4GB);
    let data = Mmap.of_fd fd Bigarray.int in
    (* keep block 0 for header etc *)
    let max_r = partition |> Prt.to_list |> List.map snd |> List.fold_left max 1 in
    let alloc_counter = ref (1+max_r) in
    let alloc () = 
      !alloc_counter |> fun r -> 
      incr alloc_counter;
      r
    in
    { fn; fd; data; alloc_counter; alloc; partition }

  (** n is the initial number of partitions of 0...max_int *)
  let create ~fn ~n =
    let alloc_counter = ref 1 in
    let alloc () = 
      !alloc_counter |> fun r -> 
      incr alloc_counter;
      r
    in
    let partition = initial_partitioning ~alloc ~n in
    create_p ~fn ~partition

  let close t = 
    Mmap.close t.data; (* closes the underlying fd *)
    (* Unix.close t.fd; *)
    (* FIXME sync partition for reopen *)
    ()

  let open_ ~fn:_ ~n:_ = failwith "FIXME"

  let mk_bucket ~data i = 
    let off = Config.blk_sz * i in
    let len = ints_per_block in
    { off; len; bucket_data=Mmap.sub data ~off ~len }

  let alloc_bucket t = 
    t.alloc () |> fun i -> 
    mk_bucket ~data:t.data i

  let add_to_bucket ~t ~bucket k v = 
    Bucket.add_to_bucket ~alloc_bucket:(fun () -> alloc_bucket t) ~bucket k v

  let find_bucket ~partition ~data k = 
    Prt.find k partition |> fun (k,r) -> 
    (* r is the partition offset within the store *)
    k,mk_bucket ~data r

  
  (* public interface: insert, find (FIXME delete) *)
    
  let insert t k v = 
    trace(fun () -> Printf.sprintf "insert: inserting %d %d\n%!" k v);
    (* find bucket *)
    let k1,bucket = find_bucket ~partition:t.partition ~data:t.data k in
    add_to_bucket ~t ~bucket k v |> function
    | `Ok -> ()
    | `Split(b1,k2,b2) -> 
      trace(fun () -> Printf.sprintf "insert: split partition %d into %d %d\n%!" k1 k1 k2);
      t.partition <- Prt.split t.partition ~k1 ~r1:(b1.off / blk_sz) ~k2 ~r2:(b2.off / blk_sz); (* FIXME ugly division by blk_sz *)
      ()  
    
  let find_opt t k = 
    let _,bucket = find_bucket ~partition:t.partition ~data:t.data k in
    Bucket.find ~bucket k

  let delete _t _k = failwith "FIXME"

  let export t = 
    let { partition; data; _ } = t in
    Prt.to_list partition |> fun krs -> 
    krs |> List.map (fun (k,_) -> find_bucket ~partition ~data k |> function (_,b) -> Bucket.export b) |> fun buckets -> 
    {partition=krs;buckets}

  let _ : t -> export_t = export

  let show t = 
    let open Sexplib.Std in
    export t |> fun e -> 
    Sexplib.Sexp.to_string_hum 
      [%message "Partition, buckets"
        ~partition:(e.partition : (int*int) list)
        ~buckets:(e.buckets: exported_bucket list)
      ]
    |> print_endline
    

end (* Make *)


module Test() = struct
  
  module Config = struct
    let max_sorted = 2
    let max_unsorted = 1
    let blk_sz = 8*8

  end

  module M = Make(Config)
  open M

  let init_partition = [(0,1);(20,2);(40,3);(60,4);(80,5);(100,5)] |> Prt.of_list

  let t = create_p ~fn:"test.db" ~partition:init_partition

  let _ = show t

  let v = (-1)
  
  let _ = insert t 0 v
  let _ = show t

  let _ = insert t 1 v
  let _ = show t

  let _ = insert t 2 v
  let _ = show t

  let _ = insert t 0 v
  let _ = show t

  let _ = insert t 1 v
  let _ = show t

(*
  let _ = insert t 2 v
  let _ = insert t 0 v
  let _ = insert t 1 v
  let _ = insert t 2 v
  let _ = insert t 3 v
  let _ = show t

  let _ = insert t 4 v
  let _ = show t
*)

end



module Test2() = struct

  module Config = struct
    (* 4096 blk_sz; 512 ints in total; 510 ints for unsorted and
       sorted; 255 kvs for unsorted and sorted *)
    
    let max_unsorted = 10
    let max_sorted = 255 - max_unsorted
    let blk_sz = 4096
  end

  module M = Make(Config)
  open M

  let t = create ~fn:"test.db" ~n:10_000

  let lim = 10_000_000

  let _ = 
    Printf.printf "Inserting %d kvs\n%!" lim;
    0 |> iter_k (fun ~k:kont i -> 
        match i < lim with
        | true -> 
          let k = Random.int64 (Int64.of_int Int.max_int) |> Int64.to_int in
          insert t k k;
          kont (i+1)
        | false -> ())

(*
make -k run_test 
time OCAMLRUNPARAM=b dune exec test/test2.exe
(Config (blk_sz 4096) (ints_per_block 512) (max_sorted 245) (max_unsorted 10)
 (bucket_size_ints 512) (bucket_size_bytes 4096))
Inserting 1000000 kvs

real	0m1.812s
user	0m1.354s
sys	0m0.318s
*)

end
