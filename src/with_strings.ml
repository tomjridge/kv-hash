(** The core implementation is for a map [int->int]; here we add some
   additional support to enable a map [string->string].

To lookup a particular string key:

- hash the key to get an int
- use an int->int map to lookup the value_i (value as int)
- value_i is an index into a table of string values, implemented as a
  single file of all values, and an offset into that file

To add a key,value:

- hash the key to get an int
- add the value to the values file to get an offset
- add (hash,offset) to the int->int map

To delete a key:

- hash the key to get an int
- delete the int from the int->int map

NOTE the old value still remains in the values file. Whether this is a
problem depends on your application: if you often delete keys there
may be substantial overhead in the values file.

*)


module Values = struct
  
  (* NOTE to avoid issues with torn writes when using mmap, we default
     to the standard pread/pwrite interface; perhaps better to use
     mmap and avoid writing over page boundaries? *)

  (* ASSUMES safe to use in and out channels on a single underlying file *)
  type t = {
    fn   : string;
    ch_r : Stdlib.in_channel;
    ch_w : Stdlib.out_channel;
  }

  (* we implement string read/write using basic in/out channel
     functions which use marshalling; FIXME format not stable long
     term *)

  let append_value t (v:string) = 
    let pos = pos_out t.ch_w in
    output_value t.ch_w v;
    flush t.ch_w;
    pos

  let read_value t ~off : string = 
    seek_in t.ch_r off;
    input_value t.ch_r

  let create ~fn = 
    (* make sure the file exists, and is of length 0 *)
    let fd = Unix.(openfile fn [O_RDWR;O_TRUNC] 0o640) in
    Unix.close fd;
    let ch_r = open_in fn in
    let ch_w = open_out fn in
    { fn; ch_r; ch_w }

  let open_ ~fn = 
    let ch_r = open_in fn in
    let ch_w = open_out fn in
    { fn; ch_r; ch_w }
end

module type INT_MAP = sig 
    type t
    val find_opt : t -> int -> int option
    val insert   : t -> int -> int -> unit
    val delete   : t -> int -> unit
  end

type 'int_map t = {
  values: Values.t;
  int_map: 'int_map;
}
  
module With_int_map(Int_map:INT_MAP) = struct
  
  let hash (s:string) = XXHash.XXH64.hash s |> Int64.to_int

  let find_opt t k = 
    let k' = hash k in
    Int_map.find_opt t.int_map k' |> function
    | None -> None
    | Some v' -> 
      Some(Values.read_value t.values ~off:v')

  let insert t k v =
    let k' = hash k in
    Values.append_value t.values v |> fun off -> 
    Int_map.insert t.int_map k' off

  let delete t k =
    let k' = hash k in
    Int_map.delete t.int_map k'

end

(** Putting it all together *)
module Make() = struct

end
