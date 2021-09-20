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
let trace (s:unit->string) = 
  print_endline (s())
  (* ignore(s); () *)
[@@warning "-27"]

let warn (s:unit->string) = 
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


