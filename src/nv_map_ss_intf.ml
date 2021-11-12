(** General non-volatile map from string to string *)


(** Operations are insert: (k,`Insert v), or delete: (k,`Delete) *)
type op = string * [ `Insert of string | `Delete ]

module type S = sig
  type t
  type nv_map_ii

  (* val xx_create   : ?buckets_fn:string -> ?values_fn:string -> unit -> t *)

  val create   : Values_file.t -> nv_map_ii -> t
  val find_opt : t -> string -> string option
  val insert   : t -> string -> string -> unit
  val batch    : t -> op list -> unit
  val close    : t -> unit
    
  (** Access to the non-volatile int->int map *)
  val get_nv_map_ii : t -> nv_map_ii

  val get_values_file : t -> Values_file.t

end

