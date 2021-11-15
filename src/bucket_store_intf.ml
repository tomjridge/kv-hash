(** A store of buckets, backed by a file/mmap *)

module type BUCKET_STORE = sig

  type raw_bucket (* = Bucket.t *)

  type t

  type bucket = {
    index:int;
    raw_bucket:raw_bucket
  }

  val create       : ?sz:int -> fn:string -> unit -> t
  (** [create] takes an optional sz parameter, which is the number of
     buckets initially allocated in the file *)

  val open_        : fn:string -> t
  val read_bucket  : t -> int -> bucket
  val write_bucket : t -> bucket -> unit
  val sync         : t -> unit
  val close        : t -> unit

end
