(** Persistent hashtable interface (for [int->int] map) *)

open Bucket_intf

type export_t = {
  partition: (int*int) list;
  buckets: exported_bucket list
}

(** "General" signature, with k and r free; we actually use this
   signature with k=int and r=int *)
module type S_kv = sig

  type raw_bucket

  type t
  type k
  type v

  (** Create with initial number of partitions; NOTE doesn't initially
     write partition or freelist since they are typically udpated by
     the merge process *)
  val create_fn : buckets_fn:string -> n:int -> t

  val create_f  : buckets_fn:string -> t

  val open_rw : 
    buckets_fn   : string -> 
    partition_fn : string -> 
    freelist_fn  : string -> 
    t

  val open_ro : 
    buckets_fn   : string -> 
    partition_fn : string -> 
    t

  val close : t -> unit

  (** [insert] should only be called from the merge process (normally
     inserts go in the log) *)
  val insert : t -> k -> v -> unit

  val find_opt : t -> k -> v option

  (** Access to subcomponents *)

  val get_partition: t -> Partition.Partition_ii.t
  val get_freelist : t -> Freelist.t

  (** Debug *)

  val export      : t -> export_t
  val show        : t -> unit
  val show_bucket : t -> k -> unit
  val get_bucket  : t -> k -> raw_bucket
  
end

(*
  val reload_partition: t -> fn:string -> unit    


  (** Private operation to reload a partition after concurrent
     modification of store by another process FIXME remove this - we
     can go via get_partition *)
  val reload_partition_and_freelist :
    t -> part_fn:string -> freelist_fn:string -> unit
*)



module type S = S_kv with type k=int and type v=int
