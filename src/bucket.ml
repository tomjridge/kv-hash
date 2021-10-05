open Bigarray
open Util
open Bucket_intf

module Make_1(C:BUCKET_CONFIG) = struct


  (* NOTE for crash-resilience a partition should be block aligned and
     block sized, and the code should work independent of the order of
     page flushes *)

  (* NOTE sorted should always have unique keys; unsorted may not have
     unique keys *)

  (* 
Layout: 

| len_sorted | k0 | v0 | ... | len_unsorted | k'0 | v'0 | ... |

k,v are sorted; k',v' are unsorted

  *)

  include C 

  let used_ints = 
    1 (* len_sorted *)
    + 2*max_sorted (* sorted kvs *)
    + 1 (* len_unsorted *)
    + 2*max_unsorted (* unsorted kvs *)

  (* let bucket_size_bytes = bucket_size_ints * Bigarray.kind_size_in_bytes Bigarray.int *)
      
  let _ = assert(used_ints <= C.len)

  let _ = assert(max_sorted >= max_unsorted)

  (** Positions/offsets within the buffer that we store certain info *)
  module Ptr = struct
    let len_sorted = 0
    let sorted_start = 1
    let len_unsorted = 2*max_sorted +1 (* position at which we store the len of unsorted *)
    let unsorted_start = len_unsorted +1
  end

  type bucket = {
    arr:int_bigarray;
    sorted:int_bigarray; (* subarray *)
    unsorted:int_bigarray; (* subarray *)
  }

  type t = bucket

  let create ?(arr=Bigarray.(Array1.init Int C_layout len (fun _ -> 0))) ()  = 
    assert(Array1.dim arr = len);
    {
      arr;
      sorted = Bigarray.Array1.sub arr Ptr.sorted_start (2*max_sorted);
      unsorted = Bigarray.Array1.sub arr Ptr.unsorted_start (2*max_unsorted);
    }  

  let get_data bucket = bucket.arr

  module With_bucket(S:sig val bucket : bucket end) = struct
    open S

    let len_sorted () = bucket.arr.{ Ptr.len_sorted }

    let set_len_sorted n = bucket.arr.{ Ptr.len_sorted } <- n
      
    let len_unsorted () = bucket.arr.{ Ptr.len_unsorted }

    let set_len_unsorted n = bucket.arr.{ Ptr.len_unsorted } <- n

    let sorted = bucket.sorted

    let unsorted = bucket.unsorted

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
      Interpolate_ii.find 
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
      let dst = Bigarray.Array1.sub bucket.arr (Ptr.sorted_start+2*max_unsorted) (2*len1) in
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
    let split_with_addition kv = 
      trace (fun () -> "split_with_addition");
      let p1,p2 = create(),create() in
      (* check clean partitions *)
      assert(p1.arr.{ Ptr.len_sorted } = 0
             && p1.arr.{ Ptr.len_unsorted } = 0);
      assert(p2.arr.{ Ptr.len_sorted } = 0
             && p2.arr.{ Ptr.len_unsorted } = 0);
      (* assert(p1.len = p.len && p2.len = p.len); *)
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
          p1.arr.{ Ptr.sorted_start + 2*i } <- k;
          p1.arr.{ Ptr.sorted_start + 2*i +1} <- v;
          incr count;
          ()
        | false -> 
          (* fill p2 *)
          let i = i - cut_point in
          p2.arr.{ Ptr.sorted_start + 2*i } <- k;
          p2.arr.{ Ptr.sorted_start + 2*i +1} <- v;
          (* no need to incr count *)
          ()
      in
      merge 
        ~ks1 ~vs1 ~len1
        ~ks2 ~vs2 ~len2
        ~set
        () |> fun n -> 
      p1.arr.{ Ptr.len_sorted } <- cut_point;
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
      p2.arr.{ Ptr.len_sorted } <- n - cut_point;
      (* get lowest key in p2 *)
      let k2 = p2.arr.{ Ptr.sorted_start } in
      (p1,k2,p2)

    let add k v = 
      trace(fun () -> "bucket.add");
      (* try to add in unsorted *)
      let len2 = len_unsorted () in
      match len2 < max_unsorted with
      | true -> (
          trace(fun () -> __LOC__);
          unsorted.{ 2*len2 } <- k;
          unsorted.{ 2*len2 +1 } <- v;
          trace(fun () -> __LOC__);
          set_len_unsorted (len2+1);
          `Ok)
      | false -> 
        trace(fun () -> __LOC__);
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
          split_with_addition (k,v) |> fun (p1,k,p2) -> 
          `Split(p1,k,p2)

    let export () = 
      trace(fun () -> "bucket.export");
      let len1 = len_sorted () in
      let len2 = len_unsorted () in
      trace(fun () -> Printf.sprintf "len1 %d; len2 %d\n" len1 len2);
      (* copy sorted into an array *)
      let kvs1 = Array.init len1 (fun i -> S.ks i, S.vs i) in
      (* copy unsorted into an array, and sort *)
      let kvs2 = Array.init len2 (fun i -> U.ks i, U.vs i) in
      { sorted=Array.to_list kvs1; unsorted=Array.to_list kvs2 }      

    (* FIXME what about delete? *)
  end (* With_bucket *)

  let insert bucket k v = 
    let open With_bucket(struct let bucket=bucket end) in
    add k v

  let _ : 
bucket ->
int -> int -> [ `Ok | `Split of bucket * int * bucket ] = insert

  let find bucket k = 
    let open With_bucket(struct let bucket=bucket end) in
    find k

  let _ : bucket -> int -> int option = find

  let export bucket = 
    let open With_bucket(struct let bucket=bucket end) in
    export()    
      
  let show t = 
    let open Sexplib.Std in
    export t |> fun e -> 
    Sexplib.Sexp.to_string_hum 
      [%message "Bucket"
          ~sorted:(e.sorted : (int*int) list)
          ~unsorted:(e.unsorted: (int*int) list)
      ]
    |> print_endline

end (* Make_1 *)

module Make_2(C:BUCKET_CONFIG) : BUCKET = Make_1(C)
