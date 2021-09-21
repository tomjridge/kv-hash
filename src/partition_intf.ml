
(** A pure interface *)
module type PURE_PARTITION = sig
  type k
  type r
  type t
  val find    : t -> k -> (k * r)
  val split   : t -> k1:k -> r1:r -> k2:k -> r2:r -> t
  val to_list : t -> (k * r) list
  val of_list : (k * r) list -> t

  val write      : t -> out_channel -> unit
  val read       : in_channel -> t
end


(** An impure interface, primarily to support "set_split_hook" *)
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

