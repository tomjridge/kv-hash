(** Persistent hashtable interface (for [int->int] map) *)

open Bucket_intf

type export_t = {
  partition: (int*int) list;
  buckets: exported_bucket list
}

(** In the following, k and r are actually int *)
module type S = sig

  type t
  type k
  type r

  val create : fn:string -> n:int -> t

  val close : t -> unit

  val insert : t -> k -> r -> unit

  val find_opt : t -> k -> r option

  (** Private operation to reload a partition after concurrent
     modification of store by another process *)
  val reload_partition: t -> fn:string -> unit

  type partition

  val get_partition: t -> partition

  val export : t -> export_t

  val show : t -> unit

  val show_bucket : t -> k -> unit
  
end
