module type FREELIST = sig
  type t
  val create : max_used:int ref -> t
  val alloc : t -> int
  val free : t -> int -> unit
  val save : t -> fn:string -> unit
  val load_no_promote : fn:string -> t
  val load_and_promote_reuse : fn:string -> t
end
