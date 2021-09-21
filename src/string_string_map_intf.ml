
(** Operations are insert: (k,`Insert v), or delete: (k,`Delete) *)
type op = string * [ `Insert of string | `Delete ]

module type S = sig
  type t
  val create : fn:string -> t
  (* val hash : string -> int *)
  val find_opt : t -> string -> string option
  val insert : t -> string -> string -> unit
  (* val delete : t -> string -> unit FIXME needs implementing *)
  val batch : t -> op list -> unit
  val close : t -> unit
    

  type phash
  val get_phash : t -> phash

(*  (** Private operation to reload partition from disk when store
     mutated by a concurrent process *)
  val reload_partition : t -> fn:string -> unit *)
end
