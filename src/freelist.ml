(** Freelist, which records the free buckets (after splitting) that
   can be reused on subsequent merges. *)

open Freelist_intf

type freelist = {
  mutable dont_reuse: int list;
  mutable do_reuse: int list;
  max_used: int ref;
}

module Make_1 = struct

  type t =  freelist
  
  let create ~max_used = {
    dont_reuse=[];
    do_reuse=[];
    max_used;
  }

  let alloc t = 
    match t.do_reuse with
    | [] -> (
        incr t.max_used;
        !(t.max_used))
    | x::xs -> 
      t.do_reuse <- xs;
      x

  let free t i = 
    t.dont_reuse <- i::t.dont_reuse;
    ()

  let save (t:t) ~fn = 
    open_out_bin fn |> fun oc -> 
    Stdlib.output_value oc t;
    close_out_noerr oc

  let load_no_promote ~fn : t = 
    open_in_bin fn |> fun ic -> 
    let t = Stdlib.input_value ic in
    close_in_noerr ic;
    t

  (** Promote dont_reuse to do_reuse, on the basis that we are
     starting a new merge *)
  let load_and_promote_reuse ~fn = 
    load_no_promote ~fn |> fun t -> 
    {t with dont_reuse=[]; do_reuse=t.do_reuse@t.dont_reuse}
  
end

module Make_2 : FREELIST with type t = freelist = Make_1

include Make_2
