(** The frontend: record updates in two logs (rotating between them). *)

open Util

let testing=true (* FIXME change when finished testing *)

type config = {
  control_filename:string;
  log0_filename:string;
  log1_filename:string;
}

module Control = struct
  type t = {
    active_log: int; (* 0 or 1 *)
    generation: int; (* increasing int *)
    last_merged: int; (* which log generation was last merged; 0
                         indicates "None"; usually we don't rotate
                         logs till previous log has merged *)
  }

  let active_log = 0
  let generation = 1
  let last_merged = 2

  let len = 3

(*
  let write arr t = 
    arr.(active_log) <- t.active_log;
    arr.(generation) <- t.generation;
    ()
*)

  let read arr = {
    active_log=arr.(active_log);
    generation=arr.(generation);
    last_merged=arr.(last_merged)
  }

end


type ('k,'v,'op) t = {
  ctl_fd      : Unix.file_descr;
  ctl_mmap    : Mmap.int_mmap;
  ctl_buf     : Mmap.int_bigarray;
  log_fds     : Unix.file_descr Array.t;
  log_mmaps   : Mmap.char_mmap Array.t;
  log_bufs    : Bigstringaf.t Array.t;
  log_contents: ('k,'op)Hashtbl.t Array.t;
  mutable active_log  : int;
  mutable active_off  : int; 
  mutable active_gen  : int; (* generation, starting from 1; 0 means "none" *)
  max_entry_size : int; (* marshalled size *)
  max_log_len : int; (* hard limit *)
}


module type W = sig
  type t
  type k
  type v
  val create : config:config -> t
  val max_off : t -> int
  val insert : t -> k -> v -> unit
  val delete : t -> k -> unit
  val close : t -> unit
end


(** NOTE k and v are typically ints (for k, a hash; for v, an offset
   into a values file *)
module Make_writer(S:sig type k[@@deriving bin_io] type v[@@deriving bin_io] end) : W with type k=S.k and type v=S.v
= struct
  include S

  module Op_snd = struct
    type op_snd = [ `Insert of v | `Delete ]
  end
  open Op_snd

  module Op = struct
    type op = Insert of k*v | Delete of k [@@deriving bin_io]
  end    
  open Op

  type nonrec t = (k,v,op_snd)t

  let const_1GiB = 1_073_741_824[@@warning "-32"]

  let const_1k = 1024[@@warning "-32"]

  let create_fd fn = Unix.(openfile fn [O_CREAT;O_TRUNC;O_RDWR] 0o640)

  let create_mmap fd = Mmap.(of_fd fd char_kind)

  let write_ctl t = 
    let arr = t.ctl_buf in
    (* FIXME what happens if this is not atomic? *)
    Control.(
      arr.{active_log} <- t.active_log;
      arr.{generation} <- t.active_gen;
      ())

  let create_contents () = Hashtbl.create 1024
    
  let create ~config = 
    let max_entry_size = 32 in (* FIXME *)
    (* let max_log_len = const_1GiB in *)
    assert(testing);
    let max_log_len = 2_000_000 in (* FIXME only for testing *)
    (* truncate log1 and log2; open control; write initial control info to control *)
    let ctl_fd = create_fd config.control_filename in
    let ctl_mmap = ctl_fd |> fun fd -> Mmap.(of_fd fd int_kind) in
    let ctl_buf = Mmap.sub ctl_mmap ~off:0 ~len:Control.len in
    let log_fns = [| config.log0_filename; config.log1_filename |] in    
    let log_fds = log_fns |> Array.map create_fd in
    let log_mmaps = log_fds |> Array.map create_mmap in
    let log_bufs = log_mmaps |> Array.map (fun m -> Mmap.sub m ~off:0 ~len:max_log_len) in
    let log_contents = [| create_contents (); create_contents () |] in 
    let active_log = 0 in
    let active_off = 0 in
    let active_gen = 0 in
    let t = { ctl_fd;ctl_mmap;ctl_buf;log_fds;log_mmaps;log_bufs;log_contents;
              active_log;active_off;active_gen;max_entry_size;max_log_len } 
    in
    write_ctl t;
    t

  (* max offset at which we attempt to read/write *)
  let max_off t = t.max_log_len - t.max_entry_size

  (* FIXME t.log_contents *)
  let rotate_log t = 
    trace(fun () -> Printf.sprintf "Rotating logs");
    t.active_log <- 1 - t.active_log;
    t.active_off <- 0;
    t.active_gen <- 1 + t.active_gen;
    t.log_contents.(t.active_log) <- create_contents ();
    write_ctl t;
    ()

  let rec insert t k v = 
    (* write to active log if enough space; update active_off; update
       log_contents; otherwise switch log, write and update ctl *)
    (* FIXME < or <= *)
    match t.active_off < max_off t with
    | true -> 
      (* can insert into active log *)
      let i = t.active_log in
      bin_write_op t.log_bufs.(i) ~pos:t.active_off (Insert(k,v)) |> fun off -> 
      t.active_off <- off;
      Hashtbl.replace t.log_contents.(i) k (`Insert v);
      write_ctl t;
      ()
    | false -> 
      (* need to rotate logs *)
      rotate_log t;
      (* NOTE no inf recursion *)
      assert(t.active_off < max_off t);
      insert t k v

  let rec delete t k = 
    (* FIXME < or <= *)
    match t.active_off < max_off t with
    | true -> 
      (* can insert into active log *)
      let i = t.active_log in
      bin_write_op t.log_bufs.(i) ~pos:t.active_off (Delete k) |> fun off -> 
      t.active_off <- off;
      Hashtbl.replace t.log_contents.(i) k `Delete;
      write_ctl t;
      ()
    | false -> 
      rotate_log t;
      (* NOTE no inf recursion *)
      assert(t.active_off < max_off t);
      delete t k      

  let close t = 
    Mmap.close t.ctl_mmap;
    t.log_mmaps |> Array.iter Mmap.close;
    ()    

end

module With_string = struct
  module S = struct
    open Bin_prot.Std
    type k = string[@@deriving bin_io]
    type v = string[@@deriving bin_io]
  end

  include Make_writer(S)
end


module Test () = struct
  open With_string

  (* create a control with 2 logs, and insert some entries *)

  let config = {
    control_filename="ctl.log";
    log0_filename="log0.log";
    log1_filename="log1.log"
  }

  let lim = 10_000_000

  let _ = 
    Printf.printf "Test starts...\n%!";
    let t = create ~config in
    0 |> iter_k (fun ~k:kont k ->
        match k < lim with 
        | true -> 
          insert t (string_of_int k) (string_of_int k);
          kont (k+1)
        | false -> ());
    Printf.printf "Test ends\n%!";    
    ()
end
