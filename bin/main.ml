(** Main binary *)

open Kv_hash
open Kv_hash.Private.Util
open Partition

(** Use a sequence so we can start printing immediately *)
let list_values ~fn = 
  let t = Values_file.open_ ~fn in
  (Values_file.list_values_seq t) |> Seq.iter (fun (s,off) -> 
      Printf.printf "%S %d\n%!" s off)


let show_partition ~fn = 
  let p = Partition_ii.read_fn ~fn in
  let xs = Partition_ii.to_list p in
  xs |> List.iter (fun (k,v) -> 
      Printf.printf "%d %d\n%!" k v)

let show_buckets ~fn =
  let bs = Bucket_store.Bucket_store0.open_ ~fn in
  1 |> iter_k (fun ~k:kont i -> 
      let b = Bucket_store0.read_bucket bs i in
      Printf.printf "Bucket %d: \n%!" i;
      Bucket0.show b.raw_bucket;
      kont (i+1))

let _ = 
  match Sys.argv |> Array.to_list |> List.tl with
  | ["list_values";fn] -> list_values ~fn
  | ["show_partition";fn] -> show_partition ~fn
  | ["show_buckets";fn] -> show_buckets ~fn
  | _ -> failwith ""
  
