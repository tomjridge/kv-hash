(** The frontend: record updates in two logs (rotating between them). *)

open Util

let testing=true (* FIXME change when finished testing *)


(* FIXME only for testing *)
(* let const_max_log_len = 4_000_000 *)

let const_1GiB = 1_073_741_824[@@warning "-32"]

let const_1k = 1024[@@warning "-32"]


module KV = struct
  open Bin_prot.Std
  type k = string[@@deriving bin_io]
  type v = string[@@deriving bin_io]
end

module Op = struct
  open KV
  type op = Insert of k*v | Delete of k [@@deriving bin_io]
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

let log_fn gen = "log_"^(string_of_int gen)




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

  let curr = 0
  let last_merg = 1
  let len = 2

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
module Writer = struct
  open KV

  module F = Control_fields

  type check_merge_t = { pid:int; gen:int }

  type t = {
    max_log_len:int;
    ctl: Control.t;
    mutable prev_map    : (k,[`Insert of v | `Delete])Hashtbl.t;
    mutable gen         : int;
    mutable curr_log    : Log_file_w.t;
    mutable curr_map    : (k,[`Insert of v | `Delete])Hashtbl.t;
    mutable check_merge : check_merge_t option;
    mutable phash       : String_string_map.t;
  }
  (** check_merge: whether we need to check the old merge has
     completed before launching a new one; the int is the pid *)

  let create ~ctl_fn ~max_log_len ~phash_fn = 
    let ctl = Control.create ~fn:ctl_fn in
    let prev_map = Hashtbl.create 1024 in
    let gen = 1 in
    let curr_log = Log_file_w.create ~fn:(log_fn gen) ~max_log_len in
    let curr_map = Hashtbl.create 1024 in
    let phash = String_string_map.create ~fn:phash_fn in
    { max_log_len;ctl;prev_map;gen;curr_log;curr_map;check_merge=None;phash }

  
  let merge_and_exit 
      ~generation 
      ~mark_merged 
      ~ops 
      ~phash = ()[@@warning "-27"]

  let switch_logs t = 
    (* need to switch logs; first check for completion of a previous
       merge *)
    begin 
      match t.check_merge with
      | None -> ()
      | Some {pid;gen} -> 
        assert(gen=t.gen-1);
        Unix.waitpid [] pid |> fun (_pid,status) -> 
        assert(status = WEXITED 0);
        (* Check also that last_merge is as we expect *)
        assert(Control.get_field t.ctl Control_fields.last_merg = gen);
        t.check_merge <- None;
        (* FIXME also need to update the new partition *)
        ()
    end;
    begin 
      Unix.fork () |> function 
      | 0 -> (* child *) 
        merge_and_exit 
          ~generation:t.gen 
          ~mark_merged:(fun gen -> Control.set_field t.ctl F.last_merg gen)
          ~ops:(Hashtbl.to_seq t.curr_map |> List.of_seq)
          ~phash:t.phash
      | pid -> (* parent *)
        t.check_merge <- Some{pid;gen=t.gen};          
        () (* NOTE will continue after this begin..end block *)
    end;
    let new_gen = t.gen +1 in
    t.prev_map <- t.curr_map;
    t.curr_log <- Log_file_w.create ~fn:(log_fn new_gen) ~max_log_len:t.max_log_len;
    t.curr_map <- Hashtbl.create 1024;
    t.phash <- t.phash; (* FIXME need to read partition from disk and update *)
    (* update gen last *)
    t.gen <- new_gen;
    Control.(set_field t.ctl F.curr t.gen); 
    ()

  let rec insert t k v = 
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

