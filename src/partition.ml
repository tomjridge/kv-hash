(** Top-level hash bucket partition, implemented using Jane St. Map


Terminology:
- partitioning: a collection of partitions which covers the space
- partition: a single partition, covering part of the space

 *)

[@@@warning "-33"](* FIXME *)

open Util
open Partition_intf


(* We could just use the range 0 to Int.max_int; this would make
   things easier, although we miss out on 2 bits per key

In this case, we don't need None, and the lowest key is 0. Every
   partition must contain a mapping for min_key.

 *)

let low = Int.zero

let high = Int.max_int


module Make(S:sig
    type k
    val compare : k -> k -> int
    val min_key : k
    type r
  end) = struct
  open S

  module K = struct 
    type t = k 
    let compare = compare
    let sexp_of_t: t -> Base.Sexp.t = fun _ -> Base.Sexp.Atom __LOC__
    (** ASSUMES this function is never called in our usecases; FIXME
        it is called; how? *)
  end

  module C = struct
    type t = K.t
    include Base.Comparator.Make(K)
  end

  let comparator : _ Base.Map.comparator = (module C)

  module Map = Base.Map

  type partition = (k,r,C.comparator_witness) Map.t 

  (* this primed version works with k options *)
  let to_list b : (k * r) list = b |> Map.to_alist 

  let of_list krs = 
    krs |> Map.of_alist_exn comparator |> fun p -> 
    assert(Map.mem p min_key);
    p

  let find k b : k * r = 
    (* find the greatest key <= k *)
    Map.closest_key b `Less_or_equal_to k |> function
    | None -> failwith "find: impossible: min_key must be a member"
    | Some (k1,r) -> 
      (k1,r)

  (* split a partition (k1,_) into (k1,r1) (k2,r2) *)
  let split (p:partition) ~k1 ~r1 ~k2 ~r2 =
    assert(Map.mem p k1);
    assert(not (Map.mem p k2));
    Map.change p k1 ~f:(function | None -> failwith "impossible" | Some _r1 -> Some r1) |> fun p -> 
    Map.set p ~key:k2 ~data:r2 |> fun p -> 
    p

  let ops = { find; split; of_list; to_list }

end

module Config = struct

  let blk_sz = 4096

  let ints_per_block = 4096 / Bigarray.kind_size_in_bytes Bigarray.int

  let int_offset ~blk = blk * ints_per_block

end
open Config


module Int = struct

  module S = struct
    type k = int
    let compare = Int.compare
    let min_key = Int.zero
    type r = int  (* r is the block offset within the data file, measured in blocks *)
  end

  include Make(S)


  module At_runtime(R:sig 
      val data: (int,int_elt) Mmap.t
    end) = struct

    (** Create an initial n-way partition, with values provided by alloc *)
    let initial_partitioning' ~alloc ~n = 
      let delta = Int.max_int / n in
      Base.List.range ~stride:delta ~start:`inclusive ~stop:`inclusive 0 ((n-1)*delta) |> fun ks -> 
      ks |> List.map (fun x -> x,alloc ()) |> fun krs -> 
      of_list krs


    (* keep block 0 for header etc *)
    let alloc = 
      let counter = ref 1 in
      fun () -> 
        !counter |> fun r -> 
        incr counter;
        r

    let initial_partitioning ~n = initial_partitioning' ~alloc ~n    


    (* NOTE we should talk in terms of partitions, but a partition
       should be a multiple of the block size (measured in
       "kind_size_in_bytes int") *)

    let partition_size = Config.ints_per_block

    (* A partition is a range within the larger data store *)
    type partition = {
      off  : int;
      len  : int;
      part_data : int_array
    }

    let find_partition k t = 
      find k t |> fun (_,r) -> 
      (* r is the partition offset within the store *)
      let off = r * partition_size in
      let len = partition_size in
      let part_data = Mmap.sub R.data ~off ~len in
      { off; len; part_data }

  end
    
end


