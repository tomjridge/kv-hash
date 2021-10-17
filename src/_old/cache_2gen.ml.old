(** Simple 2 gen cache *)

module Make_1 = struct

  (** INVARIANT: If a key is in cache_old, it should not be in
     cache_young, and vice versa *)
  type ('a,'b)t = {
    cache_young: ('a,[`Present of 'b | `Absent])Hashtbl.t; (* for recent entries *)
    cache_old  : ('a,[`Present of 'b | `Absent])Hashtbl.t; (* for entries that survive *)
  }

  let create ~young_sz ~old_sz = {
    cache_young = Hashtbl.create young_sz;
    cache_old = Hashtbl.create old_sz;
  }    

  let _ = create

  let add t k v = 
    match Hashtbl.find_opt t.cache_old k with
    | None -> 
      assert(true); (* INVARIANT hashtbl.old does not contain k, so safe to add to young *)
      Hashtbl.replace t.cache_young k v
    | Some _ -> Hashtbl.replace t.cache_old k v

  let _ : ('a, 'b) t -> 'a -> [ `Absent | `Present of 'b ] -> unit = add

  let find_opt' t k =
    match Hashtbl.find_opt t.cache_old k with
    | None -> (
        Hashtbl.find_opt t.cache_young k |> function
        | None -> None
        | Some v -> (
            (* move entry to old; preserves INVARIANT  *)
            Hashtbl.remove t.cache_young k;
            Hashtbl.replace t.cache_old k v;
            Some v))        
    | Some v -> Some v

  let _ = find_opt'

  let find_opt t k = find_opt' t k |> function
    | None -> None
    | Some `Absent -> None
    | Some (`Present v) -> Some v

  exception Exit_early

  (* FIXME hacky; may perform badly if we keep removing elements with
     the same hash range *)
  let trim tbl sz = 
    let len = Hashtbl.length tbl in
    match sz >= len with
    | true -> ()
    | false -> 
      let n = ref (len - sz) in
      (* we assume filter_map_inplace is OK with early exit *)
      try
        Hashtbl.filter_map_inplace 
          (fun _k _v -> if !n > 0 then (decr n; None) else raise Exit_early) tbl
      with Exit_early -> ()

  let maybe_trim t ~young_sz ~old_sz = 
    (if Hashtbl.length t.cache_young > young_sz then Hashtbl.clear t.cache_young);
    trim t.cache_old old_sz

end

module type S = sig
  type ('a, 'b) t
  val create: young_sz:int -> old_sz:int -> ('a, 'b) t
  val add : ('a, 'b) t -> 'a -> [ `Absent | `Present of 'b ] -> unit
  val find_opt' : ('a, 'b) t -> 'a -> [ `Absent | `Present of 'b ] option
  val find_opt : ('a, 'b) t -> 'a -> 'b option
  val maybe_trim : ('a, 'b) t -> young_sz:int -> old_sz:int -> unit
end

module Make_2 : S = Make_1

include Make_2


