(** NOTE this doesn't have a values file... it just combines
   partition and bucket *)


open Util
open Bucket_intf
open Bucket_store_intf
open Nv_map_ii_intf

module Partition_ = Partition.Partition_ii

module Make_1(Raw_bucket:BUCKET)(Bucket_store : BUCKET_STORE with type raw_bucket=Raw_bucket.t) = struct

  type raw_bucket = Bucket_store.raw_bucket

  type k = int
  type v = int

  (** Create an initial n-way partition, with values provided by alloc *)
  let initial_partitioning ~alloc ~n = 
    let stride = Int.max_int / n in
    Base.List.range ~stride ~start:`inclusive ~stop:`inclusive 0 ((n-1)*stride) |> fun ks -> 
    List.rev ks |> List.rev_map (fun x -> x,alloc ()) |> fun krs -> 
    Partition_.of_list krs

  (* Runtime handle *)
  type t = {
    bucket_store      : Bucket_store.t;
    mutable freelist  : Freelist.t;
    mutable partition : Partition_.t; (* NOTE mutable *)
  }

  let alloc t = Freelist.alloc t.freelist

  (* create with an initial partition *)
  let create_p ~buckets_fn ~partition ~min_free_blk = 
    let bucket_store = Bucket_store.create ~fn:buckets_fn () in
    let freelist = Freelist.create ~min_free:min_free_blk in
    { bucket_store; freelist; partition; }

  let create_n ~buckets_fn ~n =
    let alloc_counter = ref 1 in
    let alloc () = 
      !alloc_counter |> fun r -> 
      incr alloc_counter;
      r
    in
    let partition = initial_partitioning ~alloc ~n in
    let min_free_blk = !alloc_counter in
    create_p ~buckets_fn ~partition ~min_free_blk

  let create ~buckets_fn =
    let n = Config.config.initial_number_of_partitions in
    create_n ~buckets_fn ~n

  let close t = 
    Bucket_store.close t.bucket_store;
    (* FIXME sync partition for reopen *)
    ()

  let open_ ~fn:_ ~n:_ = failwith "FIXME persistent_hashtable.ml: open_"


  (* we have the potential for confusion if we read a bucket twice
     into different arrays; this should only happen for concurrent
     threads, when we assume one of the threads is a reader, so will
     not mutate the bucket *)
  let find_bucket t k = 
    Partition_.find t.partition k |> fun (k,blk_i) -> 
    (* blk_i is the blk index within the store *)
    let bucket = Bucket_store.read_bucket t.bucket_store blk_i in
    k,blk_i,bucket


  
  (** {2 Public interface: insert, find (FIXME delete)} *)

  (* FIXME we are syncing on each modification; may be worth caching? *)
  (** NOTE the insert function is only called in the merge process *)
  let insert t k v = 
    trace(fun () -> Printf.sprintf "insert: inserting %d %d\n%!" k v);
    (* find bucket *)
    let k1,blk_i,bucket = find_bucket t k in
    Raw_bucket.insert bucket.raw_bucket k v |> function
    | `Ok -> Bucket_store.write_bucket t.bucket_store bucket
    | `Split(kvs1,k2,kvs2) -> 
      let r1,r2 = alloc t,alloc t in 
      let b1 = Bucket_store.read_bucket t.bucket_store r1 in
      Raw_bucket.init_sorted b1.raw_bucket kvs1;
      let b2 = Bucket_store.read_bucket t.bucket_store r2 in
      Raw_bucket.init_sorted b2.raw_bucket kvs2;
      Partition_.split t.partition ~k1 ~r1 ~k2 ~r2;
      Bucket_store.write_bucket t.bucket_store b1;
      Bucket_store.write_bucket t.bucket_store b2;
      Freelist.free t.freelist blk_i; (* free the old bucket *)
      trace(fun () -> Printf.sprintf "insert: split partition %d into %d %d\n%!" k1 k1 k2);
      ()  
    
  let find_opt t k = 
    let _,_,bucket = find_bucket t k in
    Raw_bucket.find bucket.raw_bucket k


  let get_freelist t = t.freelist


  (** {2 Debugging} *)

  let export t = 
    Partition_.to_list t.partition |> fun krs -> 
    List.rev krs |> List.rev_map (fun (k,_) -> 
        find_bucket t k |> function (_,_,b) -> Raw_bucket.export b.raw_bucket) 
    |> fun buckets -> 
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


  let get_partition t = t.partition

  let show_bucket t k = 
    find_bucket t k |> fun (_,_,b) -> Raw_bucket.show b.raw_bucket

  let get_bucket t k = find_bucket t k |> fun (_,_,b) -> b.raw_bucket
    
end (* Make_1 *)

module Make_2 : functor 
  (Raw_bucket:BUCKET) 
  (Bucket_store : BUCKET_STORE with type raw_bucket=Raw_bucket.t) 
  -> Nv_map_ii_intf.S with type raw_bucket=Raw_bucket.t 
  = Make_1

module Make = Make_2

(** Standard instance *)
module Nv_map_ii0 = Make_1(Bucket.Bucket0)(Bucket_store.Bucket_store0)

module Test() = struct

(* FIXME 
  open Make_1

  module Config_ = struct
    let max_sorted = 2
    let max_unsorted = 1
    let blk_sz = 8*8

  end


  let init_partition = [(0,1);(20,2);(40,3);(60,4);(80,5);(100,5)] |> Partition_.of_list

  let t = create_p ~buckets_fn:"test.db" ~partition:init_partition

  let _ = show t

  let v = (-1)

  let _ = assert(None = find_opt t 0)
  let _ = assert(None = find_opt t 1)
  let _ = assert(None = find_opt t 20)
  let _ = assert(None = find_opt t 100)
  
  let _ = insert t 0 v
  let _ = show t
  let _ = assert(Some v = find_opt t 0)
  let _ = assert(None   = find_opt t 1)

  let _ = insert t 1 v
  let _ = show t
  let _ = assert(Some v = find_opt t 0)
  let _ = assert(Some v  = find_opt t 1)
  let _ = assert(None    = find_opt t 2)

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

*)

end



module Test2() = struct

(* FIXME
  module Config_ = struct
    (* 4096 blk_sz; 512 ints in total; 510 ints for unsorted and
       sorted; 255 kvs for unsorted and sorted *)
    
    let max_unsorted = 10
    let max_sorted = 255 - max_unsorted
    let blk_sz = 4096
  end

  module M = Make_1(Config_)
  open M

  let t = create_n ~buckets_fn:"test.db" ~n:10_000

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
(Config_ (blk_sz 4096) (ints_per_block 512) (max_sorted 245) (max_unsorted 10)
 (bucket_size_ints 512) (bucket_size_bytes 4096))
Inserting 1000000 kvs

real	0m1.812s
user	0m1.354s
sys	0m0.318s
*)
*)

end

