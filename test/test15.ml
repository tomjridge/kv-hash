(* This code exhibits an unwanted effect of buffering on an input
   channel: failure to update the buffer when the data in the
   underlying file changes. To run: `test15.exe init` then `test15.exe
   reader` (and in parallel) `test15.exe writer` *)

let rw = 
  match Sys.argv |> Array.to_list |> List.tl with
  | [ "init"] -> `Init
  | [ "reader" ] -> `Reader
  | [ "writer" ] -> `Writer
  | _ -> failwith "Wrong command line args"

let fn = "test.log"

let v0 = "initial value"
let v1 = "updated value"

let init () =
  let oc = open_out_bin fn in
  output_value oc v0;
  close_out_noerr oc
    
(* Read from position 0; print when the value read is different from v0 *)
let run_reader () = 
  let ic = open_in_bin fn in
  while true do
    seek_in ic 0;
    let s : string = input_value ic in
    if s <> v0 then print_endline s else ();
    ()
  done

let run_writer () = 
  let oc = open_out_gen [Open_wronly] 0o640 fn in
  seek_out oc 0;
  output_value oc v1;
  close_out_noerr oc
    
let _ = 
  match rw with
  | `Init -> init ()
  | `Reader -> run_reader ()
  | `Writer -> run_writer ()
