
module type PARTITION = sig
  type k
  type r
  type t
  val find    : k -> t -> (k * r)
  val split   : t -> k1:k -> r1:r -> k2:k -> r2:r -> t
  val to_list : t -> (k * r) list
  val of_list : (k * r) list -> t
end

(*
type ('k,'r,'t) partition = {
  find    : 'k -> 't -> ('k * 'r);
  split   : 't -> k1:'k -> r1:'r -> k2:'k -> r2:'r -> 't;
  to_list : 't -> ('k * 'r) list;
  of_list : ('k * 'r) list -> 't;
}  
*)

type int_elt = Bigarray.int_elt
type c_layout = Bigarray.c_layout

type int_array = (int,int_elt,c_layout)Bigarray.Array1.t
type char_array = (char,Bigarray.int8_unsigned_elt,c_layout)Bigarray.Array1.t
