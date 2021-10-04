(** Test unbuffered random write performance, to get a measure of how
   long a merge will take if fully unbuffered. *)

(* n is the number of writes *)
let n = 
  match Sys.argv |> Array.to_list |> List.tl with
  | [ n ] -> int_of_string n
  | _ -> failwith "Wrong command line args"


let fn = "test7.tmp"

let const_sz = 40_000_000_000 (* file size *)

(* create file and truncate *)
let fd = 
  Unix.(openfile fn [O_CREAT;O_RDWR] 0o600) |> fun fd -> 
  (* Unix.ftruncate fd const_sz; *)
  (* try to avoid caching *)
  ExtUnix.Specific.(fadvise fd 0 const_sz POSIX_FADV_RANDOM);
  fd

let blk_sz = 4096

(* init blk *)
let blk = Bigstringaf.create blk_sz
let _ = 
  for i = 0 to blk_sz/8 do
    Bigstringaf.set_int64_le blk 0 (Int64.of_int i)
  done

let write_blk ~fd ~blk_n ~(blk:Bigstringaf.t) = 
  ExtUnix.Specific.pwrite fd (blk_n * blk_sz) (Bigstringaf.to_string blk) 0 blk_sz |> fun n_written -> 
  assert(blk_sz = n_written);
  ()

let read_blk ~fd ~blk_n =  
  let buf = Bytes.create blk_sz in
  let n = ExtUnix.Specific.pread fd (blk_n * blk_sz) buf 0 blk_sz in
  assert(n = blk_sz || n=0);
  buf
  
let blk_max = const_sz / blk_sz 

let _ = assert(blk_max < 1 lsl 30) (* 2^30 *)

let _ = 
  Printf.printf "Starting random writes\n%!";
  let t1 = Unix.time () in
  for _i = 1 to n do
    (* write a random blk to a random part of the file *)
    let blk_n = Random.int blk_max in
    ignore(read_blk ~fd ~blk_n);
    write_blk ~fd ~blk_n ~blk    
  done;
  Unix.fsync fd;
  Unix.close fd;
  let t2 = Unix.time () in
  Printf.printf "Finished in %f\n%!" (t2 -. t1);
  ()


