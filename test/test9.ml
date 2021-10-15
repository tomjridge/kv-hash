(** Read a partition, to check for corruption etc *)

open Kv_hash

let _ = 
  Sys.argv |> Array.to_list |> List.tl |> function
  | [fn] -> 
    let _p = Partition.Partition_ii.read_fn ~fn in
    Printf.printf "Read partition\n%!";
    ()
  | _ -> failwith "need a single command line arg"
    
