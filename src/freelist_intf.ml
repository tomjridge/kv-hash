(** The freelist records which buckets are old (ie have been split
   into 2 new buckets) and can be recycled (ie reused in the NEXT
   merge).

During a merge, a bucket can become full. At this point it is split
   into two new buckets. The old bucket is then free to be
   reused. Because processes still hold the old partition (and thus,
   can access the old bucket) it is important not to reuse the bucket
   immediately. Instead, we allow the bucket to be reused during the
   NEXT merge.

Thus, during a merge we have two types of free bucket: (1) those that
   can be reused immediately (they became free during a previous
   merge) and (2) those we can only reuse from the next merge onwards.

Currently, at the end of a merge, we write the freelist state into a
   file. When we load this file at the beginning of the next merge, we
   can "promote" the buckets of type (2) so that we can reuse them
   during this merge. This is safe providing no other process has
   access to an old partition at this point.

 *)


module type FREELIST = sig
  type t

  val create : min_free:int -> t
  val alloc  : t -> int
  val free   : t -> int -> unit
  val save   : t -> fn:string -> unit

  (** Debugging: load a freelist, but don't promote newly free buckets *)
  val load_no_promote          : fn:string -> t

  (** Load a freelist, and promote newly free buckets so they can be reused *)
  val load_and_promote_reuse   : fn:string -> t

  (* In-place modification of t *)
  val reload_and_promote_reuse : t -> fn:string -> unit

  (** Debugging *)
  val debug_to_string : t -> string
end
