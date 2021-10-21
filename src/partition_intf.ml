
(** A pure interface *)
module type PURE_PARTITION = sig
  type k
  type r
  type t
  val find    : t -> k -> (k * r)
  val split   : t -> k1:k -> r1:r -> k2:k -> r2:r -> t
  val to_list : t -> (k * r) list
  val of_list : (k * r) list -> t
  val length  : t -> int
end


(** An impure interface *)
module type PARTITION = sig
  type k
  type r
  type pure_partition
  type t = { mutable partition:pure_partition }
  val find           : t -> k -> (k * r)
  val split          : t -> k1:k -> r1:r -> k2:k -> r2:r -> unit
  val to_list        : t -> (k * r) list
  val of_list        : (k * r) list -> t
  val length         : t -> int
end

