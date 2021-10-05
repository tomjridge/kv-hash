open Bigarray
open Util
open Bucket
open Persistent_hashtable_intf

module Partition_ = Partition.Partition_ii


module type CONFIG = sig
  include BUCKET_CONFIG
  val blk_sz : int (* in bytes *)
end

type int_ba_t = (int,int8_unsigned_elt,c_layout)Bigarray.Array1.t

module Make_1(Config:CONFIG) = struct

  open Config

  module Bucket_ = Bucket(Config)

  let blk_sz = Config.blk_sz

  let ints_per_block = blk_sz / Bigarray.kind_size_in_bytes Bigarray.int

  let _ = assert(Bucket_.bucket_size_bytes <= blk_sz)

  let _ = trace (fun () ->  
      let open Sexplib.Std in
      Sexplib.Sexp.to_string_hum 
        [%message "Config"
            ~blk_sz:(blk_sz : int)
            ~ints_per_block:(ints_per_block : int)
            ~max_sorted:(max_sorted : int)
            ~max_unsorted:(max_unsorted : int)
            ~bucket_size_ints:(Bucket_.bucket_size_ints : int)
            ~bucket_size_bytes:(Bucket_.bucket_size_bytes : int)
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

  let write_data t ~off ~(data:int_ba_t) = 
    let arr_c = Util.coerce_bigarray1 Ctypes.camlint Ctypes.char Bigarray.Char data in
    let len = Array1.dim arr_c in
    assert(len = blk_sz);
    Bigstring_unix.pwrite_assume_fd_is_nonblocking 
      t.fd  
      ~offset:off 
      ~pos:0 
      ~len
      arr_c |> fun n_written -> 
    assert(n_written = len);
    ()    

  let read_data t ~off = 
    let arr_c = Core.Bigstring.create blk_sz in
    Bigstring_unix.pread_assume_fd_is_nonblocking 
      t.fd  
      ~offset:off 
      ~pos:0 ~len:blk_sz arr_c |> fun n_read -> 
    assert(n_read = blk_sz);
    let arr_i = Util.coerce_bigarray1 Ctypes.char Ctypes.camlint Bigarray.Int arr_c in
    arr_i

  let create_bucket blk_i = 
    let len = ints_per_block in
    let data = Bigarray.(Array1.create Int C_layout len) in    
    { blk_i; len; bucket_data=data }

  let read_bucket t ~blk_i = 
    let off = Config.blk_sz * blk_i in
    let bucket_data = read_data t ~off in
    { blk_i; len=ints_per_block; bucket_data }        

  let alloc_bucket t = 
    t.alloc () |> fun i -> 
    create_bucket i

  let add_to_bucket ~t ~bucket k v = 
    Bucket_.add_to_bucket ~alloc_bucket:(fun () -> alloc_bucket t) ~bucket k v

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
    
  let insert t k v = 
    trace(fun () -> Printf.sprintf "insert: inserting %d %d\n%!" k v);
    (* find bucket *)
    let k1,bucket = find_bucket t k in
    add_to_bucket ~t ~bucket k v |> function
    | `Ok -> ()
    | `Split(b1,k2,b2) -> 
      trace(fun () -> Printf.sprintf "insert: split partition %d into %d %d\n%!" k1 k1 k2);
      Prt.split t.partition ~k1 ~r1:(b1.blk_i) ~k2 ~r2:(b2.blk_i);
      ()  
    
  let find_opt t k = 
    let _,bucket = find_bucket t k in
    Bucket_.find ~bucket k

  let delete _t _k = failwith "FIXME partition.ml: delete"

  let export t = 
    Prt.to_list t.partition |> fun krs -> 
    krs |> List.map (fun (k,_) -> 
        find_bucket t k |> function (_,b) -> Bucket_.export b) |> fun buckets -> 
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
