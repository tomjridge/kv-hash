(** The frontend: record updates in two logs (rotating between them). *)

(**

Sequence of events:

- log_n becomes full
- finalise log_n:
  - check previous merge (if any) has completed
  - reload partition (if changed) for working with log_(n+1) 
  - start new merge for log_n
    - merge entries
    - if partition changed, write to disk, and notify main thread    
- start new log_(n+1)

*)

open Util

let testing=true (* FIXME change when finished testing *)


module Debug = struct

  (* Show all interactions with this key *)
  let key = "z1\177\200\227\175auk3k\207\195\176\194\146\200\155@\204\138P\128\186\153\196Tc\209\016\206\139"

end

module Nv_map_ss_ = Nv_map_ss_private.Make_2

module Merge_process_ = Merge_process.Make(Nv_map_ss_)

module KV = struct
  (* open Bin_prot.Std *)
  type k = string
  type v = string
end

module Op = struct
  open KV
  type op = k * [`Insert of v | `Delete ]
end    
open Op


module Log_file_w = struct

  type t = {
    oc: Stdlib.out_channel;
    max_log_len: int
  }

  let create ~fn ~max_log_len = {
    oc=open_out_bin fn;
    max_log_len;
  }
    
  (* soft limit *)
  let can_write t = pos_out t.oc < t.max_log_len

  let write t op = output_value t.oc op

  let close t = close_out_noerr t.oc

end

module Log_file_r = struct

  type t = {
    ic: Stdlib.in_channel;
    max_len: int
  }

  let create ~fn ~max_len = {
    ic=open_in_bin fn;
    max_len;
  }
    
  (* soft limit *)
  let can_read t = pos_in t.ic < (Stdlib.in_channel_length t.ic)

  let rec read t : op = 
    try 
      input_value t.ic
    with Sys_error _e -> 
      warn(fun () -> Printf.sprintf "%s: Log_file_r read a partial value\n" __LOC__);
      (* We read a partial value; try again *)
      read t

  let close t = close_in_noerr t.ic

end



module Control_fields = struct

  (**
curr - which log generation is being written to

last_merge - which log generation was last merged; -1 indicates "None";
usually we don't switch from current log till previous log has merged *)

  let current_log = 0
  (* let last_merged_log = 1 *)
  let partition = 2  (* partition last serialized to disk is in (part_fn partition) *)
  let len = 3

  let get_field (arr:int_bigarray) field = arr.{field}

  let set_field (arr:int_bigarray) field value = 
    arr.{field} <- value

end


module Control = struct
  
  module F = Control_fields

  type t = {
    ctl_mmap         : Mmap.int_mmap;
    ctl_buf          : Mmap.int_bigarray;
  }

  let create_fd fn = Unix.(openfile fn [O_CREAT;O_TRUNC;O_RDWR] 0o640)

  let create_mmap fd = Mmap.(of_fd fd char_kind)

  let create ~fn = 
    let ctl_fd = create_fd fn in
    let ctl_mmap = ctl_fd |> fun fd -> Mmap.(of_fd fd int_kind) in
    let ctl_buf = Mmap.sub ctl_mmap ~off:0 ~len:F.len in
    {ctl_mmap;ctl_buf}

  let get_field t = F.get_field t.ctl_buf

  let set_field t = F.set_field t.ctl_buf

end

(** The writer is responsible for taking updates and recording in log,
   and periodically rotating logs and firing the merge process. *)
module Writer_1 = struct
  open KV

  module F = Control_fields

  type check_merge_t = { pid:int; gen:int; }

  type t = {
    max_log_len:int;
    ctl: Control.t;
    mutable prev_map    : (k,[`Insert of v | `Delete])Hashtbl.t;
    mutable gen         : int; (* log generation *)
    mutable curr_log    : Log_file_w.t;
    mutable curr_map    : (k,[`Insert of v | `Delete])Hashtbl.t;
    mutable check_merge : check_merge_t option;
    mutable pmap        : Nv_map_ss_.t;
    debug               : (k,v)Hashtbl.t; (* debug all inserts/deletes/finds *)
    debug_log           : Stdlib.out_channel;
  }
  (** check_merge: whether we need to check the old merge has
     completed before launching a new one; the int is the pid *)

  let create ~ctl_fn ~max_log_len ~pmap_fn = 
    let ctl = Control.create ~fn:ctl_fn in
    let prev_map = Hashtbl.create 1 in (* prev_map is empty until we complete first log *)
    let gen = 1 in
    let curr_log = Log_file_w.create ~fn:(log_fn gen) ~max_log_len in
    let curr_map = Hashtbl.create 1024 in
    let pmap = Nv_map_ss_.create ~fn:pmap_fn in
    { max_log_len;ctl;prev_map;gen;curr_log;curr_map;check_merge=None;pmap;debug=Hashtbl.create 1024; debug_log=(Stdlib.open_out_bin "debug_log") }

  let switch_logs t = 
    begin    (* check for completion of a previous merge, and reload a new
                partition if it exists *)
      match t.check_merge with
      | None -> () (* should happen only when merging the very first log *)
      | Some {pid;gen} -> 
        assert(gen=t.gen-1);
        begin    (* wait for merge *)
          let t1 = Unix.time () in
          warn (fun () -> Printf.sprintf "%s: waiting for merge\n%!" __MODULE__);
          Unix.waitpid [] pid |> fun (_pid,status) -> 
          let t2 = Unix.time () in
          warn (fun () -> Printf.sprintf "Wait completed in %f\n%!" (t2 -. t1));
          (* NOTE child merge process guaranteed to be finished at this point *)
          assert(status = WEXITED 0);
          ()
        end; 
        t.check_merge <- None;
        begin    (* Check if part_1234 exists, and if so, reload partition *)
          Sys.file_exists (part_fn gen) |> function
          | false -> ()
          | true -> 
            warn(fun () -> Printf.sprintf "Reloading partition from file part_%d\n" gen);
            Nv_map_ss_.get_phash t.pmap |> fun phash -> 
            Nv_map_ss_private.Make_1.Phash.reload_partition 
              phash
              ~fn:(part_fn gen)
        end;
    end;
    begin 
      let gen = t.gen in
      Unix.fork () |> function 
      | 0 -> 
        (* child *) 
        Merge_process_.merge_and_exit 
          ~gen
          ~pmap:t.pmap
          ~ops:(Hashtbl.to_seq t.curr_map |> List.of_seq) (* NOTE curr_map! *)
      | pid -> 
        (* parent *)
        t.check_merge <- Some{pid;gen};
        () (* NOTE parent will continue after this begin..end block *)
    end;
    let new_gen = t.gen +1 in
    (* after a batch operation, the debug state is altered in the
       merge thread, but not in the main thread FIXME could put these
       ops in the pending merge *)
    Nv_map_ss_.batch_update_debug t.pmap (Hashtbl.to_seq t.prev_map |> List.of_seq);
    t.prev_map <- t.curr_map; (* this is the map that is being merged *)
    t.curr_log <- Log_file_w.create ~fn:(log_fn new_gen) ~max_log_len:t.max_log_len;
    t.curr_map <- Hashtbl.create 1024;
    t.pmap <- t.pmap; (* NOTE partition change from a merge is dealt with above *)
    (* update gen last *)
    t.gen <- new_gen;
    Control.(set_field t.ctl F.current_log t.gen); 
    ()

  let find_opt t k = 
    (* (match k=Debug.key with | true -> debug (fun () -> Printf.sprintf "find_opt with key %S\n%!" k) | false -> ()); *)
    begin
      let map = function `Insert v -> Some v | `Delete -> None in
      Hashtbl.find_opt t.curr_map k |> function
      | Some v -> map v
      | None -> (
          Hashtbl.find_opt t.prev_map k |> function
          | Some v -> map v
          | None -> 
            Nv_map_ss_.find_opt t.pmap k)    
    end |> fun r -> 
    assert(
      let expected = Hashtbl.find_opt t.debug k in
      r = expected || begin
        Printf.printf "Debug: error detected; key is %S; value should have been %S but was %S in %s\n%!" 
          k 
          (if expected=None then "None" else Option.get expected)
          (if r=None then "None" else Option.get r)
          __MODULE__
        ;
        false end);
    Sexp_trace.append t.debug_log (k,`Find(r));    
    r

  let rec insert t k v = 
    Sexp_trace.append t.debug_log (k,`Insert(v));    
    (* (match k=Debug.key with | true -> debug (fun () -> Printf.sprintf "insert with key=%S value=%S\n%!" k v) | false -> ()); *)
    Hashtbl.replace t.debug k v;
    (* write to active log if enough space and update curr_map;
       otherwise switch to new log and merge old; then insert in new
       log *)
    match Log_file_w.can_write t.curr_log with
    | true -> 
      Log_file_w.write t.curr_log (`Insert(k,v));      
      Hashtbl.replace t.curr_map k (`Insert v);
      ()
    | false -> 
      switch_logs t;
      insert t k v

  let rec delete t k = 
    ignore(failwith "unsupported");
    match Log_file_w.can_write t.curr_log with
    | true -> 
      Log_file_w.write t.curr_log (`Delete(k));      
      Hashtbl.replace t.curr_map k `Delete;
      ()
    | false -> 
      switch_logs t;
      delete t k
    
  let close t = 
    Mmap.close t.ctl.ctl_mmap;
    Log_file_w.close t.curr_log;
    ()    

end

module type WRITER = sig
  type t 
  val create   : ctl_fn:string -> max_log_len:int -> pmap_fn:string -> t
  val find_opt : t -> string -> string option
  val insert   : t -> string -> string -> unit
  val delete   : t -> string -> unit
  val close    : t -> unit
end

module Writer_2 : WRITER = Writer_1

module Writer = Writer_2

module Test() = struct 

  let lim = 10_000_000

  let _ = 
    Printf.printf "%s: test starts\n%!" __MODULE__;
    let t = Writer.create ~ctl_fn:"ctl" ~max_log_len:32_000_000 ~pmap_fn:"pmap" in
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
   
(*

$ kv-hash $ make run_test 
time OCAMLRUNPARAM=b dune exec kv-hash/test/test6.exe
(Config (blk_sz 4096) (ints_per_block 512) (max_sorted 245) (max_unsorted 10)
 (bucket_size_ints 512) (bucket_size_bytes 4096))
Kv_hash__Frontend: test starts
Resizing 128
Kv_hash__Frontend: test ends

real    0m2.891s
user    0m2.715s
sys     0m0.156s

*)
 
end
