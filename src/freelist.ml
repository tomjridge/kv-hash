(** Freelist, which records the free buckets (after splitting) that
   can be reused on subsequent merges. *)

open Freelist_intf

type freelist = {
  mutable dont_reuse: int list;
  mutable do_reuse: int list;
  min_free: int ref;
}

module Make_1 = struct

  type t =  freelist
  
  let create ~min_free = {
    dont_reuse=[];
    do_reuse=[];
    min_free=ref min_free;
  }

  let alloc t = 
    match t.do_reuse with
    | [] -> (
        let r = !(t.min_free) in
        assert(r < Int.max_int);
        incr t.min_free;
        r)
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

  (* reload inplace *)
  let reload_and_promote_reuse t ~fn = 
    load_and_promote_reuse ~fn |> fun t' -> 
    t.dont_reuse <- t'.dont_reuse;
    t.do_reuse <- t'.do_reuse;
    t.min_free := !(t'.min_free);
    ()

  let debug_to_string t = 
    let open Sexplib.Std in
    Sexplib.Sexp.to_string_hum 
      [%message "Freelist"
          ~dont_reuse:(t.dont_reuse : int list)
          ~do_reuse:(t.do_reuse : int list)
          ~min_free:(t.min_free : int ref)
      ]    

end

module Make_2 : FREELIST with type t = freelist = Make_1

include Make_2
