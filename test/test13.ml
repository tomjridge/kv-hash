(** Test sys.time *)

let _ = 
  let t1 = Sys.time () in
  Unix.sleep 10;
  let t2 = Sys.time () in
  Printf.printf "Total system time: %f\n%!" (t2 -. t1)
  
