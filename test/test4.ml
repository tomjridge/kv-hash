(** Test 4, create a map with lots of entries then test the batch performance *)

open Kv_hash
open Private.Util

let time () = Unix.time ()

module Kv = Kv_hash.Nv_map_ss0

let initial_inserts = 1_000_000

let batch_size = 50_000
let batch_n = 10

let go () = 
  Printf.printf "Test starts\n%!";
  let t1 = time () in
  let fn = "test.db" in
  (* open db; create drops any existing db *)
  Kv.create (Values_file.create ~fn:"values.data") (Kv.Nv_map_ii_.create_f  ~buckets_fn:fn) |> fun t -> 
  let t2 = time () in
  Printf.printf "Create completed in %f\n%!" (t2 -. t1);
  0 |> iter_k (fun ~k:kont i -> 
      match i >= initial_inserts with
      | true -> ()
      | false -> 
        let k = Random.int64 (Int64.of_int Int.max_int) |> Int64.to_int in
        let k = string_of_int k in
        Kv.insert t k k;
        kont (i+1)) |> fun () -> 
  let t3 = time () in
  Printf.printf "%d inserts completed in %f\n%!" initial_inserts (t3 -. t2);
  (* now do batch inserts *)
  0 |> iter_k (fun ~k:kont m -> 
      match m >= batch_n with
      | true -> ()
      | false -> 
        let ops = List.init batch_size (fun _ -> 
            let k = Random.int64 (Int64.of_int Int.max_int) |> Int64.to_int in
            let k = string_of_int k in
            (k,`Insert k))
        in
        Kv.batch t ops;
        kont (m+1)) |> fun () -> 
  let t4 = time () in
  Printf.printf "%d batch inserts completed in %f\n%!" (batch_n * batch_size) (t4 -. t3);  
  Kv.close t;
  let t5 = time () in
  Printf.printf "Close in %f\n%!" (t5 -. t4);
  ()

let _ = go ()
