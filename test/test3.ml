(** Test the string->string map *)

open Kv_hash
open Util

let time () = Unix.time ()

module Kv = Kv_hash.Nv_map_ss

let lim = 1_000_000

let go () = 
  Printf.printf "Test starts\n%!";
  let t1 = time () in
  let fn = "test.db" in
  (* open db; create drops any existing db *)
  Kv.create ~buckets_fn:fn () |> fun t -> 
  let t2 = time () in
  Printf.printf "Create completed in %f\n%!" (t2 -. t1);
  0 |> iter_k (fun ~k:kont i -> 
      match i >= lim with
      | true -> ()
      | false -> 
        let k = Random.int64 (Int64.of_int Int.max_int) |> Int64.to_int in
        let k = string_of_int k in
        Kv.insert t k k;
        kont (i+1)) |> fun () -> 
  let t3 = time () in
  Printf.printf "%d inserts completed in %f\n%!" lim (t3 -. t2);
  Kv.close t;
  let t4 = time () in
  Printf.printf "Close in %f\n%!" (t4 -. t3);
  ()

let _ = go ()
