(** Main binary *)

open Kv_hash

(** Use a sequence so we can start printing immediately *)
let list_values ~fn = 
  let t = Values_file.open_ ~fn in
  (Values_file.list_values_seq t) |> Seq.iter (fun (s,off) -> 
      Printf.printf "%S %d\n%!" s off)


let _ = 
  match Sys.argv |> Array.to_list |> List.tl with
  | ["list_values";fn] -> list_values ~fn
  | _ -> failwith ""
  
