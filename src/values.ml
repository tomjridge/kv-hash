(** A file containing a collection of "values" (the values stored in
   the [int->int] map are the offsets of the real values within the
   values file). *)

module type VALUES = 
sig
  type t 
  val append_value : t -> string -> int
  val read_value : t -> off:int -> string
  val create : fn:string -> t
  val open_ : fn:string -> t
  val flush : t -> unit
  val close : t -> unit
end

module Make_1 = struct
  
  (* NOTE to avoid issues with torn writes when using mmap, we default
     to the standard pread/pwrite interface; perhaps better to use
     mmap and avoid writing over page boundaries? *)

  (* ASSUMES safe to use in and out channels on a single underlying file *)
  type t = {
    fn   : string;
    ch_r : Stdlib.in_channel;
    ch_w : Stdlib.out_channel;
  }

  (* we implement string read/write using basic in/out channel
     functions which use marshalling; FIXME format not stable long
     term *)

  (* NOTE because we are using marshalling, append_value and
     read_value are polymorphic in the value type, so we don't have to
     specialize to strings here; may be useful if the value type is
     something other than string *)
  let append_value t (v:string) = 
    let pos = pos_out t.ch_w in
    output_value t.ch_w v;
    flush t.ch_w;
    pos

  let read_value t ~off : string = 
    seek_in t.ch_r off;
    input_value t.ch_r

  let create ~fn = 
    (* make sure the file exists, and is of length 0 *)
    let fd = Unix.(openfile fn [O_CREAT;O_RDWR;O_TRUNC] 0o640) in
    Unix.close fd;
    let ch_r = open_in fn in
    let ch_w = open_out fn in
    { fn; ch_r; ch_w }

  let open_ ~fn = 
    let ch_r = open_in fn in
    let ch_w = open_out fn in
    { fn; ch_r; ch_w }

  let flush t = flush t.ch_w

  let close t = 
    flush t;
    close_in_noerr t.ch_r;
    close_out_noerr t.ch_w;
    ()
end

module Make_2 : VALUES = Make_1

include Make_2
