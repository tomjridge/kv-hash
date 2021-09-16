module type S = sig
  type t
  val create : fn:string -> t
  (* val hash : string -> int *)
  val find_opt : t -> string -> string option
  val insert : t -> string -> string -> unit
  val delete : t -> string -> unit
  val close : t -> unit
end
