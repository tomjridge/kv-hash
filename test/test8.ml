(** Test bigarray casting using https://discuss.ocaml.org/t/cast-bigarray-kind/8469/8 *)

let arr1_i = Bigarray.(Array1.init Int C_layout 512 (fun i -> i))

let _ = 
  Printf.printf "Bigarray int size in bytes: %d\n" Bigarray.(kind_size_in_bytes int);
  assert(Bigarray.(kind_size_in_bytes int) = 8) (* for 64bit arch *)

let _arr2_c = 
  Ctypes.bigarray_start Ctypes.array1 arr1_i |> fun (pi:int Ctypes.ptr) -> 
  Ctypes.(coerce (ptr int) (ptr char) pi) |> fun (pc:char Ctypes.ptr) -> 
  Ctypes.bigarray_of_ptr 
    Ctypes.array1 
    (Bigarray.Array1.dim arr1_i * Ctypes.(sizeof int))
    Bigarray.Char
    pc |> fun (arr:Bigstringaf.t) -> 
  arr

(* t1 and t2 are ctypes kinds; t2_kind is a normal bigarray kind *)
let coerce_bigarray1 t1 t2 t2_kind arr = 
  Ctypes.bigarray_start Ctypes.array1 arr |> fun (pi:'t1 Ctypes.ptr) -> 
  Ctypes.(coerce (ptr t1) (ptr t2) pi) |> fun (pc:'t2 Ctypes.ptr) -> 
  Ctypes.bigarray_of_ptr (* this function forces C layout *) 
    Ctypes.array1 
    ((Bigarray.Array1.dim arr * Ctypes.(sizeof t1)) / Ctypes.(sizeof t2))
    t2_kind
    pc |> fun arr -> 
  arr

(* NOTE c_layout of the result *)
let _ : 
't1 Ctypes_static.typ ->
't2 Ctypes_static.typ ->
('t2, 'a) Bigarray.kind ->
('t1, 'b, 'c) Bigarray.Array1.t ->
('t2, 'a, Bigarray.c_layout) Bigarray.Array1.t
= coerce_bigarray1

let _ = Printf.printf "Size of int: %d; size of char: %d\n" 
    Ctypes.(sizeof int)
    Ctypes.(sizeof char)

let _ = assert(Ctypes.(sizeof int) = 4)

let _ = Printf.printf "NOTE that Ctypes.sizeof int is 4 not 8! So use camlint not int!\n"

let arr2_c = coerce_bigarray1 Ctypes.camlint Ctypes.char Bigarray.Char arr1_i

let _ : (char, Bigarray.int8_unsigned_elt, Bigarray.c_layout) Bigarray.Array1.t = arr2_c

let _ = 
  Printf.printf "Size of arr1: %d; size of arr2: %d\n%!" 
    (Bigarray.Array1.dim arr1_i)
    (Bigarray.Array1.dim arr2_c);    
  assert(Bigarray.Array1.dim arr2_c = 8 * Bigarray.Array1.dim arr1_i)

let arr3_i = coerce_bigarray1 Ctypes.char Ctypes.camlint Bigarray.Int arr2_c

let _ : (int, Bigarray.int_elt, Bigarray.c_layout) Bigarray.Array1.t = arr3_i

let _ = assert(Bigarray.Array1.dim arr3_i = Bigarray.Array1.dim arr1_i)

