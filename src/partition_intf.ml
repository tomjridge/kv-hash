
module type PARTITION = sig
  type k
  type r
  type t
  val find    : t -> k -> (k * r)
  val split   : t -> k1:k -> r1:r -> k2:k -> r2:r -> unit
  val to_list : t -> (k * r) list
  val of_list : (k * r) list -> t

  val set_split_hook : t -> (unit -> unit) -> unit

  val write      : t -> out_channel -> unit
  val read       : in_channel -> t
end

(*
type int_elt = Bigarray.int_elt
type c_layout = Bigarray.c_layout

type int_bigarray = (int,int_elt,c_layout)Bigarray.Array1.t
type char_bigarray = (char,Bigarray.int8_unsigned_elt,c_layout)Bigarray.Array1.t
*)

