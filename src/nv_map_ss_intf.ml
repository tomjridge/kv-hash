(** General non-volatile map from string to string *)


(** Operations are insert: (k,`Insert v), or delete: (k,`Delete) *)
type op = string * [ `Insert of string | `Delete ]

module type S = sig
  type t
  val create   : fn:string -> t
  val find_opt : t -> string -> string option
  val insert   : t -> string -> string -> unit
  val batch    : t -> op list -> unit
  val close    : t -> unit
    
  (** Access to the non-volatile int->int map *)
  type nv_map_ii
  val get_nv_map_ii : t -> nv_map_ii

end


  (* val delete : t -> string -> unit FIXME needs implementing *)
  (* val hash : string -> int *)

(*  (** Private operation to reload partition from disk when store
     mutated by a concurrent process *)
  val reload_partition : t -> fn:string -> unit *)
