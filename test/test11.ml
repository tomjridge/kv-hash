(** Stress test the frontend *)

open Kv_hash.Private.Util
open Kv_hash.Frontend

let max_log_len = 
  Sys.getenv_opt "MAX_LOG_LEN" |> function
  | None -> 32_000_000
  | Some x -> int_of_string x

let lim = 1_000_000_000 (* ie, "forever" *)

let _ = 
  Printf.printf "%s: test starts\n%!" __MODULE__;
  let t = Writer.create ~max_log_len () 
  in
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


