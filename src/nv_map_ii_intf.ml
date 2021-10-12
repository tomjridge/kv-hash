(** Persistent hashtable interface (for [int->int] map) *)

open Bucket_intf

type export_t = {
  partition: (int*int) list;
  buckets: exported_bucket list
}

(** "General" signature, with k and r free; we actually use this
   signature with k=int and r=int *)
module type S_kr = sig

  type t
  type k
  type r

  val create : fn:string -> n:int -> t

  val close : t -> unit

  val insert : t -> k -> r -> unit

  val find_opt : t -> k -> r option

  (** Private operation to reload a partition after concurrent
     modification of store by another process FIXME remove this - we
     can go via get_partition *)
  val reload_partition: t -> fn:string -> unit    


  (** Access to subcomponents *)

  val get_partition: t -> Partition.Partition_ii.t


  (** Debug *)

  val export : t -> export_t

  val show : t -> unit

  val show_bucket : t -> k -> unit
  
end

module type S = S_kr with type k=int and type r=int
