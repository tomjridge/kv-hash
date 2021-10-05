(** Main interface types for bucket *)

open Util

(*
(* A bucket is stored at (off,len) within the larger data store;
   FIXME could just return bucket_data? do we need off and len? *)
type bucket = {
  blk_i : int; (* index of backing block *)
  len   : int; (* length in number of ints *)
  bucket_data : int_bigarray
}
*)

module type BUCKET_CONFIG = sig
  val max_sorted   : int  
  val max_unsorted : int
  val len          : int (* length of backing int bigarray; must be
                            large enough to include
                            2*(max_sorted+max_unsorted)+2 *)
end

include struct
  (* for debugging *)
  open Sexplib.Std
  type exported_bucket = {
    sorted: (int*int) list;
    unsorted: (int*int) list
  }[@@deriving sexp]
end

module type BUCKET = sig

  include BUCKET_CONFIG

  type bucket

  val create : ?arr:int_bigarray -> unit -> bucket

  val data : bucket -> int_bigarray (* guaranteed to be of size len *)

  type k := int
  type v := int

  val find   : bucket -> k -> v option
  val insert : alloc_bucket:(unit -> bucket) -> bucket -> k -> v -> [ `Ok | `Split of bucket * k * bucket ]
  val show   : bucket -> unit
  val export : bucket -> exported_bucket

end
