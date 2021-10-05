open Util
open Mmap

(* A bucket is stored at (off,len) within the larger data store;
   FIXME could just return bucket_data? do we need off and len? *)
type bucket = {
  blk_i : int; (* index of backing block *)
  len   : int; (* length in number of ints *)
  bucket_data : int_bigarray
}

include struct
  (* for debugging *)
  open Sexplib.Std
  type exported_bucket = {
    sorted: (int*int) list;
    unsorted: (int*int) list
  }[@@deriving sexp]
end

module Interpolate_ = Interpolate(struct type k = int type v = int end)

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
      let len = len_unsorted () in
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
