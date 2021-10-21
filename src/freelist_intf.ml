module type FREELIST = sig
  type t
  val create : min_free:int -> t
  val alloc : t -> int
  val free : t -> int -> unit
  val save : t -> fn:string -> unit
  val load_no_promote : fn:string -> t
  val load_and_promote_reuse : fn:string -> t

  (* In-place modification of t *)
  val reload_and_promote_reuse: t -> fn:string -> unit
end
