(** NOTE this doesn't have a values file... it just combines
   partition and bucket *)


open Util
open Bucket_intf
open Bucket_store_intf
open Nv_map_ii_intf

module Partition_ii = Partition.Partition_ii

(*
module Create_opts = struct

  (* FIXME add min_free_blk to partition *)

  (* f - file; i - int; p - partition *)
  type t = 
    | O_ff of { bucket_fn:string; partition_fn:string }
    | O_fi of { bucket_fn:string; partition_n:int }
    | O_fp of { bucket_fn:string; partition:Partition_ii.t }
end
*)


module Util_ = struct
  (** Create an initial n-way partition, with values provided by alloc *)
  let initial_partitioning ~alloc ~n = 
    let stride = Int.max_int / n in
    Base.List.range ~stride ~start:`inclusive ~stop:`inclusive 0 ((n-1)*stride) |> fun ks -> 
    List.rev ks |> List.rev_map (fun x -> x,alloc ()) |> fun krs -> 
    Partition_ii.of_list krs
end
open Util_

module Make_1(Raw_bucket:BUCKET)(Bucket_store : BUCKET_STORE with type raw_bucket=Raw_bucket.t) = struct

  type raw_bucket = Bucket_store.raw_bucket

  type k = int
  type v = int


  (* Runtime handle *)
  type t = {
    bucket_store      : Bucket_store.t;
    mutable freelist  : Freelist.t;
    mutable partition : Partition_ii.t; (* NOTE mutable *)
  }

  (* abbrev *)
  let alloc t = Freelist.alloc t.freelist

  (* FIXME just have a single create with values, not fnames *)

  (* create with an initial partition and min_free_blk *)
  let create_fpn ~buckets_fn ~partition ~min_free_blk = 
    let bucket_store = Bucket_store.create ~fn:buckets_fn () in
    let freelist = Freelist.create ~min_free:min_free_blk in
    { bucket_store; freelist; partition; }

  let create_fp ~buckets_fn ~partition =
    let min_free_blk = Partition_ii.suc_max_value partition in
    create_fpn ~buckets_fn ~partition ~min_free_blk

  let create_fn ~buckets_fn ~n =
    let alloc_counter = ref 1 in
    let alloc () = 
      !alloc_counter |> fun r -> 
      incr alloc_counter;
      r
    in
    let partition = initial_partitioning ~alloc ~n in
    let min_free_blk = !alloc_counter in
    create_fpn ~buckets_fn ~partition ~min_free_blk

  let create_f ~buckets_fn =
    let n = Config.config.initial_number_of_partitions in
    create_fn ~buckets_fn ~n

  let close t = 
    Bucket_store.close t.bucket_store;
    (* FIXME sync partition for reopen *)
    ()

  (* NOTE there should only be one rw process *)
  let open_rw ~buckets_fn ~partition_fn ~freelist_fn = 
    let buckets = Bucket_store.open_ ~fn:buckets_fn in
    let partition = Partition_ii.read_fn ~fn:partition_fn in
    let freelist = Freelist.load_no_promote ~fn:freelist_fn in
    { bucket_store=buckets; freelist; partition }
    
  (* FIXME if we open from an RO instance, some of the files may no
     longer exist, so we should take care to catch exceptions, close
     opened fds etc *)
  let open_ro ~buckets_fn ~partition_fn = 
    let buckets = Bucket_store.open_ ~fn:buckets_fn in
    let partition = Partition_ii.read_fn ~fn:partition_fn in
    let freelist = Freelist.create ~min_free:Int.max_int in 
    (* this freelist will throw an error on attempt to allocate *)
    { bucket_store=buckets; freelist; partition }


  (* we have the potential for confusion if we read a bucket twice
     into different arrays; this should only happen for concurrent
     threads, when we assume one of the threads is a reader, so will
     not mutate the bucket *)
  let find_bucket t k = 
    Partition_ii.find t.partition k |> fun (k,blk_i) -> 
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
      Partition_ii.split t.partition ~k1 ~r1 ~k2 ~r2;
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
    Partition_ii.to_list t.partition |> fun krs -> 
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

