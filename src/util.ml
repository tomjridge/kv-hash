(** Essentially the Y combinator; useful for anonymous recursive
   functions. The k argument is the recursive callExample:

{[
  iter_k (fun ~k n -> 
      if n = 0 then 1 else n * k (n-1))

]}


 *)
let iter_k f (x:'a) =
  let rec k x = f ~k x in
  k x

let dest_Some = function
  | None -> failwith "dest_Some"
  | Some x -> x

(* trace execution *)
let trace = 
  match Config.debug with
  | false -> fun s -> ignore(s); ()
  | true -> fun s -> print_endline (s())


let warn (s:unit->string) = 
  print_endline (s())

let debug (s:unit->string) = 
  print_endline (s())


(** This builds a comparator for Jane St. Base.Map. Not sure this is
   the intended procedure. *)
module Make_comparator(S:sig type k val compare:k->k->int end) = struct
  open S

  module K = struct
    type t = k 
    let compare = compare
    let sexp_of_t: t -> Base.Sexp.t = fun _ -> Base.Sexp.Atom __LOC__
    (** ASSUMES this function is never called in our usecases; FIXME
        it is called; how? *)
  end
  include K

  module C = struct
    type t = K.t
    include Base.Comparator.Make(K)
  end
  include C

  let comparator : _ Base.Map.comparator = (module C)
end



(** Interpolate once, then scan. ASSUMES we are using Stdlib.( < > =) etc *)
module Interpolate(S:sig
    type k = int
    type v
  end) 
= struct
  open S

  let find ~(len:int) ~(ks:int->k) ~(vs:int->v) k =
    match len = 0 with true -> None | false -> 
    let low_k = ks 0 in
    let high_k = ks (len -1) in
    match () with 
    | _ when k < low_k -> None
    | _ when k > high_k -> None
    | _ when k = low_k -> Some (vs 0)
    | _ when k = high_k -> Some (vs (len -1))
    | _ -> 
      (* interpolate between low_k and high_k *)
      let high_low = float_of_int (high_k - low_k) in
      (* delta is between 0 and 1 *)
      let delta = float_of_int (k - low_k) /. high_low in
      let i = int_of_float (delta *. (float_of_int len)) in              
      (* clip to len-1 *)
      let i = min (len - 1) i in
      match k = ks i with
      | true -> Some (vs i)
      | false -> 
        begin
          let dir = if k < ks i then -1 else +1 in
          (* scan from i+dir, in steps of dir, until we know we can't find k *)
          i+dir |> iter_k (fun ~k:kont j -> 
              (* we can forget about the first and last positions... *)
              match j >= len-1 || j<= 0 with
              | true -> None
              | false -> 
                let kj = ks j in
                match k=kj with
                | true -> Some (vs j)
                | false -> 
                  match (k > kj && dir = -1) || (k < kj && dir = 1) with
                  | true -> None
                  | false -> kont (j+dir))
        end

  let _ : len:k -> ks:(k -> k) -> vs:(k -> v) -> k -> v option = find
end

module Interpolate_ii = Interpolate(struct type k = int type v = int end)


(* Merge two sorted sequences; ks2 take precedence; perhaps it would
   be cleaner to actually use sequences *)
module Merge(S:sig
    type k
    type v
    val ks1 : int -> k
    val vs1 : int -> v
    val len1 : int
    val ks2 : int -> k
    val vs2 : int -> v
    val len2: int
    val set : int -> k -> v -> unit
  end) = struct
  open S

  let merge_rest ks vs j len i = 
    trace(fun () -> Printf.sprintf "merge_rest: %d %d %d\n%!" j len i);
    assert(j <= len);
    (j,i) |> iter_k (fun ~k:kont (j,i) -> 
        match j >= len with 
        | true -> i (* return the position after the merged result *)
        | false -> 
          set i (ks j) (vs j);
          kont (j+1,i+1))

  let merge () = 
    (0,0,0) |> iter_k (fun ~k:kont (i1,i2,i) -> 
        match () with
        | _ when i1 >= len1 -> merge_rest ks2 vs2 i2 len2 i
        | _ when i2 >= len2 -> merge_rest ks1 vs1 i1 len1 i
        | _ -> 
          let k1,k2 = ks1 i1, ks2 i2 in
          match k1 < k2 with 
          | true -> 
            set i k1 (vs1 i1);
            kont (i1+1,i2,i+1)
          | false -> 
            match k1 = k2 with
            | true -> 
              set i k2 (vs2 i2);
              (* NOTE we drop from ks1 as well *)
              kont (i1+1,i2+1,i+1)
            | false -> 
              assert(k2 < k1);
              set i k2 (vs2 i2);
              kont (i1,i2+1,i+1))             
end

let merge (type k v) ~ks1 ~vs1 ~len1 ~ks2 ~vs2 ~len2 ~set = 
  let module Merge = Merge(struct 
      type nonrec k=k 
      type nonrec v=v 
      let ks1,vs1,len1,ks2,vs2,len2,set = ks1,vs1,len1,ks2,vs2,len2,set
    end) 
  in
  Merge.merge


module Mmap = Tjr_mmap.Mmap

(** Write to mmap using a bin_writer; allocate len space initially; if
   not enough space, increase the buffer size and try again. *)
let write_increasing ~(bin_write_v:_ Bin_prot.Write.writer) ~len ~mmap ~off ~v =
  len |> iter_k (fun ~k len -> 
      try
        bin_write_v (Mmap.sub mmap ~off ~len) ~pos:0 v (* returns length of written value *)
      with Bin_prot.Common.Buffer_short -> k (len*2))

let read_increasing ~(bin_read_v:_ Bin_prot.Read.reader) ~len ~mmap ~off =
  len |> iter_k (fun ~k len -> 
      try
        bin_read_v (Mmap.sub mmap ~off ~len) ~pos_ref:(ref 0)
      with Bin_prot.Common.Buffer_short -> k (len*2))
      
  

type char_bigarray = (char,Bigarray.int8_unsigned_elt,Bigarray.c_layout)Bigarray.Array1.t

type int_bigarray = (int,Bigarray.int_elt,Bigarray.c_layout)Bigarray.Array1.t

type int_ba_t = (int,Bigarray.int_elt,Bigarray.c_layout)Bigarray.Array1.t


(** The kind of the mmap'ed array; see Bigarray.kind *)
type ('a,'b) kind = ('a,'b) Bigarray.kind

let char_kind : (char,Bigarray.int8_unsigned_elt) kind = Bigarray.Char

let int_kind : (int,Bigarray.int_elt) kind = Bigarray.Int


let log_fn gen = "log_"^(string_of_int gen)

let part_fn gen = "part_"^(string_of_int gen)

let freelist_fn gen = "freelist_"^(string_of_int gen)

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

let write_int_ba ~fd ~off (data:int_ba_t) = 
  let arr_c = coerce_bigarray1 Ctypes.camlint Ctypes.char Bigarray.Char data in
  let len = Bigarray.Array1.dim arr_c in
  (* assert(len = blk_sz); *)
  Bigstring_unix.pwrite_assume_fd_is_nonblocking 
    fd  
    ~offset:off 
    ~pos:0 
    ~len
    arr_c |> fun n_written -> 
  assert(n_written = len);
  ()    

let read_int_ba ~blk_sz ~fd ~off = 
  let arr_c = Core.Bigstring.create blk_sz in
  Bigstring_unix.pread_assume_fd_is_nonblocking 
    fd  
    ~offset:off 
    ~pos:0 ~len:blk_sz arr_c |> fun n_read -> 
  assert(n_read = 0 || n_read = blk_sz);
  (if n_read = 0 then Bigarray.Array1.fill arr_c (Char.chr 0));
  let arr_i = coerce_bigarray1 Ctypes.char Ctypes.camlint Bigarray.Int arr_c in
  arr_i


let sorted_int_ba arr = 
  let b = ref true in
  for i = 0 to Bigarray.Array1.dim arr -1 -1 do
    b := !b && (arr.{i} < arr.{i+1})
  done;
  !b
  

module Map_i = Map.Make(struct type t = int let compare: int -> int -> int = Int.compare end)


(** NOTE taken from kv-lite/trace.ml *)
module Sexp_trace = struct

  (** Some utility code for traces *)

  open Sexplib.Std

  type op = string * [ `Insert of string | `Delete | `Find of string option ][@@deriving sexp]

  type ops = op list[@@deriving sexp]

  let append oc op = 
    Sexplib.Sexp.output_hum oc (sexp_of_op op);
    output_string oc "\n\n";
    Stdlib.flush oc;
    ()

  let write fn ops = 
    let oc = Stdlib.open_out_bin fn in
    Sexplib.Sexp.output_hum oc (sexp_of_ops ops);
    Stdlib.close_out_noerr oc

  let read fn =
    let ic = Stdlib.open_in_bin fn in
    Sexplib.Sexp.input_sexp ic |> ops_of_sexp

end


include Config.Consts


(** A mutable Lru for string->string map; at the moment this does not
   record negative information (this key is not present) but perhaps
   it should FIXME *)
module Lru_ss = struct

  module K = struct
    include String
    let hash (s:string) : int = Hashtbl.hash s
  end
  
  module V = struct
    type t = string
    let weight : t -> int = fun _ -> 1
  end

  (** NOTE a mutable Lru; you have to call trim explicitly *)
  include Lru.M.Make(K)(V)

  type tbl = (string,[`Insert of string | `Delete]) Hashtbl.t

  (** Adjust the Lru, given a recent set of operations; assuming the
     lru is small wrt. the operations, we just loop through the lru
     updating any values that have been updated by the operations; new
     operations do not get added automatically; we do not trim *)
  let batch_adjust t (tbl:tbl) = 
    (* Don't rely on Lru allowing mutations during iteration *)
    let kvs = to_list t in
    kvs |> List.iter (fun (k,_) -> 
        Hashtbl.find_opt tbl k |> function
        | Some (`Insert v) -> add k v t
        | Some (`Delete) -> failwith "Lru_ss: delete: not supported"
        | None -> ());
    ()
    
end

(** default file perm: u+rw, g+r *)
let perm0 = 0o640

let int_sz_bytes = Bigarray.(kind_size_in_bytes int)

let _ = assert(int_sz_bytes = 8)



let wait_for_file_to_exist ~fn = 
  0 |> iter_k (fun ~k:kont n -> 
      Sys.file_exists fn |> function
      | false -> (
          Thread.yield (); 
          (if n mod 1_000_000_000 = 0 then 
             Printf.printf "WARNING: %s: thread waited \
                            for 1B iterations for file %s\n%!" __MODULE__ fn);
          kont (n+1))
      | true -> 
        ())

