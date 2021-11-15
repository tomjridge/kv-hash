(** A file containing a collection of "values" (the values stored in
   the [int->int] map are the offsets of the real values within the
   values file).

NOTE since fds are inherited, this is not safe to use in a
   multiprocess environment - the use of seek for read_value causes
   problems; use reload in the child process instead

*)

open Util

module type S = 
sig
  type t 
  val append_value : t -> string -> int
  val read_value : t -> off:int -> string
  val create : fn:string -> t
  val open_ : fn:string -> t
  val flush : t -> unit
  val close : t -> unit

  val reload: t -> unit
  (** close all existing channels and reopen; this is used for child
     processes to avoid concurrency issues arising from file offsets
     and seeking *)

  val list_values: t -> (string * int) list

  (** NOTE this function (and the resulting sequence) uses seek on the
     underlying [in_channel], so is not safe to use at the same time
     as other operations *)
  val list_values_seq: t -> (string * int) Seq.t
end

module Make_1 = struct
  
  (* NOTE to avoid issues with torn writes when using mmap, we default
     to the standard pread/pwrite interface; perhaps better to use
     mmap and avoid writing over page boundaries? *)

  (* ASSUMES safe to use in and out channels on a single underlying
     file, no caching etc; FIXME change to use raw fds *)
  type t = {
    fn   : string;
    mutable ch_r : Stdlib.in_channel;
    mutable ch_w : Stdlib.out_channel;
    mutable pos_w: int; (* where we are in the out_channel *)
  }

  (* we track where we are in the out_channel because we want to
     ensure we never go backwards and overwrite an existing value *)

  (* we implement string read/write using basic in/out channel
     functions which use marshalling; FIXME format not stable long
     term *)

  (* NOTE because we are using marshalling, append_value and
     read_value are polymorphic in the value type, so we don't have to
     specialize to strings here; may be useful if the value type is
     something other than string *)
  let append_value t (v:string) =     
    let pos = pos_out t.ch_w in
    assert(pos = t.pos_w);
    assert(pos = out_channel_length t.ch_w);
    output_value t.ch_w v;
    flush t.ch_w;
    let x = pos_out t.ch_w in
    assert(x > t.pos_w);
    t.pos_w <- x;
    pos

  let read_value t ~off : string = 
    seek_in t.ch_r off;
    input_value t.ch_r

  let out_flags = [Open_wronly;Open_append]
  (* NOTE plain open_out truncates the file :( *)

  let create ~fn = 
    (* make sure the file exists, and is of length 0 *)
    let fd = Unix.(openfile fn [O_CREAT;O_RDWR;O_TRUNC] 0o640) in
    Unix.close fd;
    let ch_r = open_in fn in
    let ch_w = open_out_gen out_flags 0o640 fn in
    let pos_w = 0 in
    { fn; ch_r; ch_w; pos_w }

  let open_ ~fn = 
    let ch_r = open_in fn in
    let ch_w = open_out_gen out_flags 0o640 fn in 
    let pos_w = out_channel_length ch_w in
    seek_out ch_w pos_w;
    { fn; ch_r; ch_w; pos_w }

  let flush t = flush t.ch_w

  let close t = 
    flush t;
    close_in_noerr t.ch_r;
    close_out_noerr t.ch_w;
    ()

  let reload t =
    close t;
    open_ ~fn:t.fn |> fun { ch_r; ch_w; pos_w; _ } -> 
    t.ch_r <- ch_r;
    t.ch_w <- ch_w;
    t.pos_w <- pos_w;
    ()

  let list_values t =
    let max_pos = in_channel_length t.ch_r in
    seek_in t.ch_r 0;
    [] |> iter_k (fun ~k:kont xs -> 
        let pos = pos_in t.ch_r in
        match pos = max_pos with
        | true -> List.rev xs
        | false -> 
          let s : string option = 
            try Some(input_value t.ch_r) with _ -> None
          in
          match s with
          | None -> 
            failwith (Printf.sprintf 
                        "%s: list_values attempted to input_value but \
                         something went wrong in file %s at position \
                         %d\n%!" 
                        __MODULE__ t.fn pos)
          | Some s -> kont ((s,pos)::xs))

  let list_values_seq t =
    let max_pos = in_channel_length t.ch_r in
    seek_in t.ch_r 0;
    () |> iter_k (fun ~k:kont () -> 
        let pos = pos_in t.ch_r in
        match pos = max_pos with
        | true -> Seq.empty
        | false ->          
          let s : string option = 
            try Some(input_value t.ch_r) with _ -> None
          in
          match s with
          | None -> 
            failwith (Printf.sprintf 
                        "%s: list_values attempted to input_value but \
                         something went wrong in file %s at position \
                         %d\n%!" 
                        __MODULE__ t.fn pos)
          | Some s -> 
            Seq.cons (s,pos) (fun () -> kont () ()))
    
end

module Make_2 : S = Make_1

include Make_2
