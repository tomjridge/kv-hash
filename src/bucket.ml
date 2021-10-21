open Bigarray
open Util
open Bucket_intf


module Make_1(C:BUCKET_CONFIG) = struct
  
  open struct
    type k = int
    type v = int
  end

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


  (* FIXME perhaps avoid all the merging of unsorted etc by creating a
     map and checking the length of that *)

  include C 

  let used_ints = 
    1 (* len_sorted *)
    + 2*max_sorted (* sorted kvs *)
    + 1 (* len_unsorted *)
    + 2*max_unsorted (* unsorted kvs *)

  (* let bucket_size_bytes = bucket_size_ints * Bigarray.kind_size_in_bytes Bigarray.int *)
      
  let _ = assert(used_ints <= C.bucket_length_in_ints)

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

  let to_bigarray bucket = bucket.arr

  (* FIXME should we do further checks here? eg that sorted really are
     sorted? that the elts are within a given range? *)
  let of_bigarray ba = 
    assert(ba.{Ptr.len_sorted} <= max_sorted);
    assert(ba.{Ptr.len_unsorted} <= max_unsorted);
    assert(Bigarray.Array1.dim ba = bucket_length_in_ints);
    {
      arr=ba;
      sorted=Array1.sub ba Ptr.sorted_start (2*max_sorted);
      unsorted=Array1.sub ba Ptr.unsorted_start (2*max_unsorted);
    }    
    

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

    (* convert to sorted array; include some additional elts xs *)
    let unsorted_to_sorted xs = 
      let len = len_unsorted () in
      let m = Map_i.of_seq (List.to_seq xs) in
      (0,m) |> iter_k (fun ~k:kont (i,m) -> 
          match i < len with
          | false -> m
          | true -> 
            Map_i.add (U.ks i) (U.vs i) m |> fun m -> 
            kont (i+1,m)) |> fun m -> 
      Map_i.to_seq m |> fun seq -> 
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

    let sorted_to_map () = 
      let len = len_sorted () in
      (Map_i.empty,0) |> iter_k (fun ~k:kont (m,i) -> 
          match i < len with
          | true -> kont (Map_i.add (S.ks i) (S.vs i) m, i+1)
          | false -> m)

    (* m is an initial map - typically the result of sorted_to_map *)
    let unsorted_to_map m = 
       let len = len_unsorted () in
       (m,0) |> iter_k (fun ~k:kont (m,i) -> 
          match i < len with
          | true -> kont (Map_i.add (U.ks i) (U.vs i) m, i+1)
          | false -> m)      

    (** The kv parameter is an extra key-value that we need to add to
       the split. Return value is a triple (kvs1,k,kvs2) where kvs1 <
       k <= kvs2, and |kvs1|,|kvs2| < max_sorted, and kvs1 and kvs2
       are sorted i.e. safe to construct a new bucket for kvs1 and
       kvs2, separated by k *)
    let split_with_addition (k,v) = 
      trace (fun () -> "split_with_addition");
      let len1 = len_sorted () in
      let len2 = len_unsorted () in
      assert(not (len1 + len2 <= max_sorted));
      sorted_to_map () |> fun m1 -> 
      unsorted_to_map m1 |> fun m2 -> 
      Map_i.add k v m2 |> fun m3 -> 
      (* now split those less than k, and those >= k *)
      let len = Map_i.cardinal m3 in
      Map_i.bindings m3 |> fun kvs -> 
      Base.List.split_n kvs (len/2) |> fun (kvs1,kvs2) -> 
      match kvs2 with
      | [] -> failwith "split_with_addition" (* FIXME *)
      | (k,_v)::_ -> 
        assert(List.length kvs1 < max_sorted); (* FIXME? *)
        assert(List.length kvs2 < max_sorted); (* FIXME? *)
        (kvs1,k,kvs2)

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

    (* initialize a new bucket from a sorted list of kvs *)
    let init_sorted kvs = 
      let len = List.length kvs in
      assert(len <= max_sorted);
      (* NOTE following assertions may not hold if bucket is recycled *)
      (* assert(len_unsorted() = 0); *)
      (* assert(len_sorted() = 0); *)
      kvs |> List.iteri (fun i (k,v) -> 
          sorted.{2*i} <- k;
          sorted.{2*i +1} <- v);
      set_len_sorted len;
      set_len_unsorted 0;
      ()

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
int -> int -> [ `Ok | `Split of (k*v)list * k * (k*v)list ] = insert

  let find bucket k = 
    let open With_bucket(struct let bucket=bucket end) in
    find k

  let _ : bucket -> int -> int option = find


  let init_sorted bucket kvs =
    let open With_bucket(struct let bucket=bucket end) in
    init_sorted kvs    


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


  module With_debug() = struct

    let _ = Printf.printf "%s: debugging bucket operations\n%!" __MODULE__

    (* Specification just a map for the time being *)

    let to_map bucket = 
      let len_s = (2*bucket.arr.{Ptr.len_sorted}) in
      let sorted = Bigarray.Array1.sub bucket.sorted 0 len_s in
      (* check that the keys (only) are sorted *)
      assert(
        begin
          0 |> iter_k (fun ~k i -> 
            match i >= Bigarray.Array1.dim sorted -2 with
              | true -> true
              | false -> 
                sorted.{i} < sorted.{i+2} && k (i+2))
        end || begin          
          let open Sexplib.Std in
          Sexplib.Sexp.to_string_hum 
            [%message "Bucket.sorted is not sorted"
                ~sorted:(Array.init len_s (fun i -> sorted.{i}) : int array)
            ]
          |> print_endline;
          false end          
      );
      let unsorted = Bigarray.Array1.sub bucket.unsorted 0 (2*bucket.arr.{Ptr.len_unsorted}) in
      (Map_i.empty,0) |> iter_k (fun ~k (m,i) -> 
          match i < Bigarray.Array1.dim sorted with
          | true -> k (Map_i.add sorted.{i} sorted.{i+1} m, i+2)
          | false -> m) |> fun m -> 
      (m,0) |> iter_k (fun ~k (m,i) -> 
          match i < Bigarray.Array1.dim unsorted with
          | true -> k (Map_i.add unsorted.{i} unsorted.{i+1} m, i+2)
          | false -> m) |> fun m -> 
      m

    let kvs_to_map kvs = Map_i.of_seq (List.to_seq kvs)
      
    let insert bucket k v = 
      let m1 = to_map bucket in
      insert bucket k v |> fun r -> 
      match r with 
      | `Ok -> 
        let m2 = to_map bucket in
        assert(Map_i.add k v m1 |> Map_i.equal Int.equal m2);
        r
      | `Split(b1,_k,b2) -> 
        let m21 = kvs_to_map b1 in
        let m22 = kvs_to_map b2 in
        begin (* check separated *)
          Map_i.max_binding_opt m21 |> function
          | None -> ()
          | Some max -> 
            Map_i.min_binding_opt m22 |> function
            | None -> ()
            | Some min -> assert(max < min); ()
        end;
        begin (* check bindings agree *)
          assert( (Map_i.add k v m1 |> Map_i.bindings)
                  = (Map_i.bindings m21 @ Map_i.bindings m22));
        end;
        r

    let find bucket k = 
      let m1 = to_map bucket in
      find bucket k |> fun vopt -> 
      assert(vopt = Map_i.find_opt k m1);
      vopt

  end (* With_debug *)
end (* Make_1 *)



module Make_2(C:BUCKET_CONFIG) : BUCKET = Make_1(C)

module Make = Make_2

(** Standard bucket; other buckets may be used for testing *)
module Bucket0 = struct
  module Config_ = struct
    let max_sorted = Config.config.bucket_sorted
    let max_unsorted = Config.config.bucket_unsorted
    let bucket_length_in_ints = Config.config.blk_sz / int_sz_bytes
  end

  include Make(Config_)
end
