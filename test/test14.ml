(* rw is `Reader or `Writer *)
let rw = 
  match Sys.argv |> Array.to_list |> List.tl with
  | [ "reader" ] -> `Reader
  | [ "writer" ] -> `Writer
  | _ -> failwith "Wrong command line args"

module Test = Kv_hash.Log_file.Test(struct let limit = 1_000_000 let fn = "test.log" end)

let _ = 
  match rw with
  | `Reader -> Test.run_reader ()
  | `Writer -> Test.run_writer ()
