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

module Nv_map_ss_ = Nv_map_ss_private.Make_2(Bucket.Bucket0)

module Merge_process_ = Merge_process.Make(struct
    module Nv_map_ii_ = Nv_map_ss_.Nv_map_ii_
    module Nv_map_ss_ = Nv_map_ss_
  end)

module KV = struct
  open Bin_prot.Std
  type k = string[@@deriving bin_io]
  type v = string[@@deriving bin_io]
end

module Op = struct
  open KV
  type op = k * [`Insert of v | `Delete ][@@deriving bin_io]
end    
open Op

(* Version using out_channel 
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
*)

module Log_file_w = struct
  type t = {
    fn          :string;
    max_sz      : int;
    fd          : Unix.file_descr;
    mmap        : Mmap.char_mmap;
    buf         : Mmap.char_bigarray;
    mutable pos : int;
  }

  let create ~fn ~max_sz = 
    let fd = Unix.(openfile fn [O_CREAT;O_RDWR;O_TRUNC] 0o640) in
    let mmap = Mmap.of_fd fd Mmap.char_kind in
    let buf = Mmap.sub mmap ~off:0 ~len:max_sz in
    {
      fn;
      max_sz;    
      fd;
      mmap;
      buf;
      pos=0
    }

  let write t op = 
    try begin
      bin_write_op t.buf ~pos:t.pos op |> fun pos' -> 
      t.pos <- pos';
      `Ok
    end
    with Bin_prot.Common.Buffer_short -> `Buffer_short

  (* FIXME may want to have a "closed" flag which we check *)
  let close t = 
    Mmap.close t.mmap;
    t.pos <- -1;
    ()
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
    mutable nv_map_ss   : Nv_map_ss_.t;
    lru_ss              : Lru_ss.t;
    debug               : (k,v)Hashtbl.t; (* debug all inserts/deletes/finds *)
    debug_log           : Stdlib.out_channel;
  }
  (** check_merge: whether we need to check the old merge has
     completed before launching a new one; the int is the pid

      lru_ss: an Lru that sits behind the logs but in front of nv_map_ss
  *)

  let create 
      ?max_log_len:(max_log_len=Config.config.max_log_length) 
      ?ctl_fn:(ctl_fn=Config.config.ctl_fn)
      ?buckets_fn:(buckets_fn=Config.config.bucket_store_fn)
      ?values_fn:(values_fn=Config.config.values_fn)
      () 
    = 
    let ctl = Control.create ~fn:ctl_fn in
    let prev_map = Hashtbl.create 1 in (* prev_map is empty until we complete first log *)
    let gen = 1 in
    let curr_log = 
      Log_file_w.create ~fn:(log_fn gen) ~max_sz:max_log_len |> fun log -> 
      Control.(set_field ctl F.current_log gen);
      log
    in
    let curr_map = Hashtbl.create (max_log_len / (8+8))  in  (* approx how many entries in log for tezos *)
    let nv_map_ss = Nv_map_ss_.create ~buckets_fn ~values_fn () in
    let lru_ss = Lru_ss.create Config.config.lru_capacity in
    let _ =
      (* make sure we write an initial partition to partition 0 *)
      Nv_map_ss_.get_nv_map_ii nv_map_ss |> fun ii -> 
      Nv_map_ss_.Nv_map_ii_.get_partition ii |> fun part -> 
      Partition.Partition_ii.write_fn part ~fn:(part_fn 0);
      Control.(set_field ctl F.partition 0);
      ()
    in
    { max_log_len;
      ctl;
      prev_map;
      gen;
      curr_log;
      curr_map;
      check_merge=None;
      nv_map_ss;
      lru_ss;
      debug=Hashtbl.create 1024; 
      debug_log=(Stdlib.open_out_bin "debug_log") }

  let _ = create

  let open_ 
      ?max_log_len:(max_log_len=Config.config.max_log_length) 
      ?ctl_fn:(ctl_fn=Config.config.ctl_fn)
      ?buckets_fn:(buckets_fn=Config.config.bucket_store_fn)
      ?values_fn:(values_fn=Config.config.values_fn)
      () 
    =
    (* get current log and part from the ctl file *)
    let log_n,part_n = 1,2 in
    (* check log_n, log_predn exist *)
    (* merge log_predn if it exists *)
    (* FIXME how do we know how far we are in log_n? perhaps prefer to use oc *)
    ()[@@warning "-26-27"]

  let switch_logs t = 
    begin    
      (* check for completion of a previous merge, and reload a new
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
          let part_mmmm = part_fn gen in
          Sys.file_exists part_mmmm |> function
          | false -> ()
          | true -> 
            warn(fun () -> Printf.sprintf "Reloading partition from file %s\n" part_mmmm);
            Nv_map_ss_.get_nv_map_ii t.nv_map_ss |> fun map_ii -> 
            Nv_map_ss_.Nv_map_ii_.reload_partition 
              map_ii
              ~fn:part_mmmm;
            Control.(set_field t.ctl F.partition gen);
            ()
        end;
    end;
    begin 
      (* before we merge, we take care to update the Lru with the
         entries to be merged; we also trim the Lru at this point;
         FIXME the Lru is potentially unbounded if finds occur without
         merging *)
      Lru_ss.batch_adjust t.lru_ss t.curr_map;
      Lru_ss.trim t.lru_ss
    end;
    begin 
      let gen = t.gen in
      Unix.fork () |> function 
      | 0 -> (
          (* child *) 
          Merge_process_.merge_and_exit 
            ~gen
            ~nv_map_ss:t.nv_map_ss
            ~ops:(Hashtbl.to_seq t.curr_map |> List.of_seq)) (* NOTE curr_map! *)
      | pid -> (
          (* parent *)
          t.check_merge <- Some{pid;gen};
          ()) (* NOTE parent will continue after this begin..end block *)
    end;
    let new_gen = t.gen +1 in
    (* after a batch operation, the debug state is altered in the
       merge thread, but not in the main thread FIXME could put these
       ops in the pending merge *)
    Nv_map_ss_.batch_update_debug t.nv_map_ss (Hashtbl.to_seq t.prev_map |> List.of_seq);
    t.prev_map <- t.curr_map; (* this is the map that is being merged *)
    Log_file_w.close t.curr_log;
    t.curr_log <- Log_file_w.create ~fn:(log_fn new_gen) ~max_sz:t.max_log_len;
    t.curr_map <- Hashtbl.create 1024;
    t.nv_map_ss <- t.nv_map_ss; (* NOTE partition change from a merge is dealt with above *)
    (* update gen last *)
    t.gen <- new_gen;
    Control.(set_field t.ctl F.current_log t.gen); 
    ()

  (* FIXME at the moment the Lru can grow without bound, if there are repeated finds *)
  let find_opt t k = 
    begin
      let map = function `Insert v -> Some v | `Delete -> None in
      Hashtbl.find_opt t.curr_map k |> function
      | Some v -> map v
      | None -> (
          Hashtbl.find_opt t.prev_map k |> function
          | Some v -> map v
          | None -> 
            Lru_ss.find k t.lru_ss |> function
            | None -> (
              let v = Nv_map_ss_.find_opt t.nv_map_ss k in
              match v with 
              | None -> None
              | Some v -> (Lru_ss.add k v t.lru_ss; Some v))
            | Some v -> 
              Lru_ss.promote k t.lru_ss;
              Some v)
    end

  let rec insert t k v = 
    (* write to active log if enough space and update curr_map;
       otherwise switch to new log and merge old; then insert in new
       log *)
    match Log_file_w.write t.curr_log (k,`Insert v) with
    | `Ok -> 
      Hashtbl.replace t.curr_map k (`Insert v);
      ()
    | `Buffer_short -> 
      switch_logs t;
      insert t k v


  let delete _t _k = failwith "unsupported"
(* FIXME
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
*)
    
  let close t = 
    (* wait for the merge to finish *)
    begin match t.check_merge with
      | None -> ()
      | Some {pid; _} -> 
        Unix.waitpid [] pid |> fun (_pid,status) -> 
        assert(status = WEXITED 0);
        ()
    end;
    Mmap.close t.ctl.ctl_mmap;
    Log_file_w.close t.curr_log;
    ()    

  (** Debugging versions *)
  module With_debug() = struct
    let _ = Printf.printf "%s: using With_debug version\n%!" __MODULE__

    let find_opt t k = 
      find_opt t k |> fun r -> 
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

    let insert t k v =
      insert t k v;
      Sexp_trace.append t.debug_log (k,`Insert(v));    
      Hashtbl.replace t.debug k v;
      () 
  end

end

module type WRITER = sig
  type t 
  val create   : ?max_log_len:int -> ?ctl_fn:string -> ?buckets_fn:string -> ?values_fn:string -> unit -> t
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
    let t = Writer.create 
        ~max_log_len:32_000_000 ~ctl_fn:"ctl" ~buckets_fn:"buckets" ~values_fn:"values" () 
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
