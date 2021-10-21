(** The freelist records which buckets are old (ie have been split
   into 2 new buckets) and can be recycled (ie reused in the NEXT
   merge).

NOTE on block recycling: Other processes (eg read only processes)
   should always start a lookup by checking whether they have the
   current partition. In addition they must always check for partition
   change just before returning any result; if the partition has
   changed since the process started the operation, the process must
   retry the operation; this ensures that recycled blocks are not
   misinterpreted leading to incorrect results.

   Alternatively, if we can ensure that any other process can execute
   a single operation in less time than it takes from the start of one
   merge to the start of another, then we are (probably) safe, since
   no block will be recycled during that time (we recycle old blocks
   on the NEXT merge).  *)


module type FREELIST = sig
  type t

  val create : min_free:int -> t
  val alloc  : t -> int
  val free   : t -> int -> unit
  val save   : t -> fn:string -> unit

  val load_no_promote          : fn:string -> t
  val load_and_promote_reuse   : fn:string -> t

  (* In-place modification of t *)
  val reload_and_promote_reuse : t -> fn:string -> unit

  (** Debugging *)
  val debug_to_string : t -> string
end
