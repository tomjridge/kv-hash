(** The frontend: record updates in two logs (rotating between them). *)

type config = {
  control_filename:string;
  log0_filename:string;
  log1_filename:string
}

module Control = struct
  open Bin_prot.Std
  type t = {
    active_log: int; (* 0 or 1 *)
    generation: int; (* increasing int *)
  }[@@deriving bin_io]

  let write ~buf ~off t = 
    assert(Bigstringaf.length buf > 20);
    ignore(bin_write_t buf ~pos:off t)

  let read ~buf ~off = 
    assert(Bigstringaf.length buf > 20);
    bin_read_t buf ~pos_ref:(ref off)
end


type ('k,'v,'op) t = {
  ctl_fd      : Unix.file_descr;
  ctl_mmap    : Mmap.char_mmap;
  ctl_buf     : Bigstringaf.t;
  log_fds     : Unix.file_descr Array.t;
  log_mmaps   : Mmap.char_mmap Array.t;
  log_bufs    : Bigstringaf.t Array.t;
  log_contents: ('k,'op)Hashtbl.t Array.t;
  mutable active_log  : int;
  mutable active_off  : int; 
  mutable active_gen  : int;
  max_entry_size : int; (* marshalled size *)
  max_log_len : int; (* hard limit *)
}

(** NOTE k and v are typically ints (for k, a hash; for v, an offset
   into a values file *)
module Make_writer(S:sig type k[@@deriving bin_io] type v[@@deriving bin_io] end) 
= struct
  open S

  module Op_snd = struct
    type op_snd = [ `Insert of v | `Delete ]
  end
  open Op_snd

  module Op = struct
    open Bin_prot.Std
    type op = Insert of k*v | Delete of k [@@deriving bin_io]
  end    
  open Op

  let const_1GiB = 1_073_741_824

  let create_fd fn = Unix.(openfile fn [O_CREAT;O_TRUNC;O_RDWR] 0o640)

  let create_mmap fd = Mmap.(of_fd fd char_kind)

  let write_ctl (t:_ t) = 
    let {active_log;active_gen=generation;_} = t in
    Control.write ~buf:t.ctl_buf ~off:0 {Control.active_log;generation}
    
  let create ~config = 
    let max_entry_size = 1024 in
    let max_log_len = const_1GiB in
    (* truncate log1 and log2; open control; write initial control info to control *)
    let ctl_fd = create_fd config.control_filename in
    let ctl_mmap = ctl_fd |> create_mmap in
    let ctl_buf = Mmap.sub ctl_mmap ~off:0 ~len:1024 in
    let log_fns = [| config.log0_filename; config.log1_filename |] in    
    let log_fds = log_fns |> Array.map create_fd in
    let log_mmaps = log_fds |> Array.map create_mmap in
    let log_bufs = log_mmaps |> Array.map (fun m -> Mmap.sub m ~off:0 ~len:max_log_len) in
    let log_contents = [| Hashtbl.create 1024; Hashtbl.create 1024 |] in 
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

  let rotate_log t = 
    t.active_log <- 1 - t.active_log;
    t.active_off <- 0;
    t.active_gen <- 1 + t.active_gen;
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
      write_ctl t;
      ()
    | false -> 
      rotate_log t;
      (* NOTE no inf recursion *)
      assert(t.active_off < max_off t);
      delete t k      

end
