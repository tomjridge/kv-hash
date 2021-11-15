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

