open Kv_hash.Frontend
open Kv_hash.Private.Util

module Test() = struct 

  let lim = 10_000_000

  let _ = 
    Printf.printf "%s: test starts\n%!" __MODULE__;
    let t = Writer.create ~max_log_len:32_000_000 () in
    begin
      0 |> iter_k (fun ~k:kont i -> 
          match i < lim with
          | false -> ()
          | true -> 
            (if i mod 1_000_000 = 0 then Printf.printf "Reached %d\n%!" i);
            Writer.insert t (string_of_int i) (string_of_int i);
            kont (i+1))
    end;
    Writer.close t;
    Printf.printf "%s: test ends\n%!" __MODULE__;
   
(*

$ kv-hash $ make run_test 
time OCAMLRUNPARAM=b dune exec kv-hash/test/test6.exe
(Config (blk_sz 4096) (ints_per_block 512) (max_sorted 245) (max_unsorted 10)
 (bucket_size_ints 512) (bucket_size_bytes 4096))
Kv_hash__Frontend: test starts
Resizing 128
Kv_hash__Frontend: test ends

real    0m2.891s
user    0m2.715s
sys     0m0.156s

*)
 
end

module _ = Test()
