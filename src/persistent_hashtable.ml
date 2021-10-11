(* open Bigarray *)
open Util
open Bucket_intf
(* open Bucket *)
open Persistent_hashtable_intf

module Partition_ = Partition.Partition_ii


module type CONFIG = sig
  val max_sorted:int
  val max_unsorted:int
  val blk_sz : int (* in bytes *)
end

module Make_1(Config:CONFIG) = struct

  open Config
  let blk_sz = Config.blk_sz

  module Bucket_config = struct
    include Config
    let len = blk_sz / Bigarray.(kind_size_in_bytes Int)
  end

  (* Raw bucket *)
  module Rawb' = Bucket.Make(Bucket_config)
  (* Add debugging at runtime; FIXME expensive? prefer simple module expression? *)
  let rawb = Sys.getenv_opt "DEBUG_BUCKET" |> function
    | None -> (module Rawb' : BUCKET with type t = Rawb'.t)
    | Some _ -> (module (struct
                  include Rawb'
                  include Rawb'.With_debug()
                end))
  module Rawb = (val rawb)

  type bucket = { blk_i:int; rawb: Rawb.t }

  let ints_per_block = blk_sz / Bigarray.kind_size_in_bytes Bigarray.int

  let _ = trace (fun () ->  
      let open Sexplib.Std in
      Sexplib.Sexp.to_string_hum 
        [%message "Config"
            ~blk_sz:(blk_sz : int)
            ~ints_per_block:(ints_per_block : int)
            ~max_sorted:(max_sorted : int)
            ~max_unsorted:(max_unsorted : int)
            (* ~bucket_size_ints:(Bucket_.bucket_size_ints : int) *)
            (* ~bucket_size_bytes:(Bucket_.bucket_size_bytes : int) *)
        ])      

  type k = int
  type r = int

  module Prt = Partition_      

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
    alloc_counter     : int ref;
    alloc             : unit -> int;
    mutable partition : Prt.t; (* NOTE mutable *)
    (* add_to_bucket     : bucket -> int -> int -> [ `Ok | `Split of bucket * int * bucket ]; *)
  }

  let const_4GB = 4_294967296

  (* create with an initial partition *)
  let create_p ~fn ~partition = 
    (* Bigstring_unix requires fd to be non-blocking for pwrite *)
    Core.Unix.(openfile ~mode:[O_CREAT;O_RDWR; O_TRUNC;O_NONBLOCK] fn) |> fun fd -> 
    Unix.ftruncate fd (const_4GB);
    (* let data = Mmap.of_fd fd Bigarray.int in *)
    (* keep block 0 for header etc *)
    let max_r = partition |> Prt.to_list |> List.map snd |> List.fold_left max 1 in
    let alloc_counter = ref (1+max_r) in
    let alloc () = 
      !alloc_counter |> fun r -> 
      incr alloc_counter;
      r
    in
    { fn; fd; alloc_counter; alloc; partition }

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
    Unix.close t.fd;
    (* FIXME sync partition for reopen *)
    ()

  let open_ ~fn:_ ~n:_ = failwith "FIXME persistent_hashtable.ml: open_"

  let sync_bucket t (b:bucket) : unit = 
    write_int_ba ~fd:t.fd ~off:(b.blk_i * blk_sz) (Rawb.get_data b.rawb)

  let create_bucket t ~blk_i = 
    let rawb = Rawb.create_empty () in
    let b = { blk_i; rawb } in
    sync_bucket t b;
    b

  let read_bucket t ~blk_i = 
    let arr = read_int_ba ~fd:t.fd ~blk_sz ~off:(blk_sz * blk_i) in    
    { blk_i; rawb=Rawb.create_nonempty arr }

  (* we have the potential for confusion if we read a bucket twice
     into different arrays; this should only happen for concurrent
     threads, when we assume one of the threads is a reader, so will
     not mutate the bucket *)
  let find_bucket t k = 
    Prt.find t.partition k |> fun (k,blk_i) -> 
    (* blk_i is the blk index within the store *)
    let bucket = read_bucket t ~blk_i in
    k,bucket
  
  (* public interface: insert, find (FIXME delete) *)

  (* FIXME we are syncing on each modification; may be worth caching? *)
  let insert t k v = 
    trace(fun () -> Printf.sprintf "insert: inserting %d %d\n%!" k v);
    (* find bucket *)
    let k1,bucket = find_bucket t k in
    Rawb.insert bucket.rawb k v |> function
    | `Ok -> sync_bucket t bucket
    | `Split(b1,k2,b2) -> 
      let r1,r2 = t.alloc(),t.alloc() in      
      let b1,b2 = {blk_i=r1;rawb=b1 },{blk_i=r2;rawb=b2} in
      trace(fun () -> Printf.sprintf "insert: split partition %d into %d %d\n%!" k1 k1 k2);
      Prt.split t.partition ~k1 ~r1 ~k2 ~r2;
      sync_bucket t b1;
      sync_bucket t b2;
      ()  
    
  let find_opt t k = 
    let _,bucket = find_bucket t k in
    Rawb.find bucket.rawb k

  let delete _t _k = failwith "FIXME partition.ml: delete"

  let export t = 
    Prt.to_list t.partition |> fun krs -> 
    krs |> List.map (fun (k,_) -> 
        find_bucket t k |> function (_,b) -> Rawb.export b.rawb) |> fun buckets -> 
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
    
  let reload_partition t ~fn =     
    let ic = open_in_bin fn in
    let partition = Prt.read ic in
    let max_r = 
      Partition_.to_list partition |> fun krs -> 
      krs |> List.map snd |> List.fold_left max 0
    in
    t.alloc_counter := 1+max_r; (* FIXME or max_r? *)
    t.partition <- partition

  let get_partition t = t.partition

  let show_bucket t k = 
    find_bucket t k |> fun (_,b) -> Rawb.show b.rawb
    

end (* Make *)

module type S = 
  Persistent_hashtable_intf.S 
  with type k=int 
   and type r=int
   and type partition := Partition_.t

module Make_2(Config:CONFIG) : S = Make_1(Config)


module Test() = struct
  
  module Config = struct
    let max_sorted = 2
    let max_unsorted = 1
    let blk_sz = 8*8

  end

  module M = Make_1(Config)
  open M

  let init_partition = [(0,1);(20,2);(40,3);(60,4);(80,5);(100,5)] |> Prt.of_list

  let t = create_p ~fn:"test.db" ~partition:init_partition

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

end



module Test2() = struct

  module Config = struct
    (* 4096 blk_sz; 512 ints in total; 510 ints for unsorted and
       sorted; 255 kvs for unsorted and sorted *)
    
    let max_unsorted = 10
    let max_sorted = 255 - max_unsorted
    let blk_sz = 4096
  end

  module M = Make_1(Config)
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
