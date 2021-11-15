(** Main interface types for bucket

A bucket is (abstractly) a small set of (key,value) pairs. In fact,
   keys and values are ints, but we stick with the use of "key" and
   "value".

A bucket is represented concretely by an array of size sz_b bytes. The
   size sz_b is chosen such that it is equal to the page size of the
   OS, typically 4096 bytes (say, P=4096). On a 64bit machine, we can
   store 4096/8 = 512 ints in such an array.

Why choose the size according to P? We hope that P-aligned writes of
   size P are handled atomically by the OS, filesystem and storage
   device. We write buckets to storage in their entirety, at a
   P-aligned address in a file. We are aware that the OS may re-order
   the bucket writes. However, for correctness we only rely on the
   atomicity of bucket writes, not their relative ordering when they
   hit storage. After a sequence of bucket updates (following a large
   batch operation, say), we issue a final sync to the backing
   file. If we crash before the sync, at least the buckets are
   internally consistent and (per bucket) represent either the old
   state or the new state.

The keys in a bucket are typically all in some sub-range of the key
   space (in this case, there is some small range l <= k < h such that
   keys k lie within this range). Keys are assumed to be roughly
   uniformly distributed (so, the keys are actually hash values from
   some reasonable hash function). For a given bucket, the sub-range
   is not represented within the bucket itself, but rather, in some
   external structure (the partition).

The exact details of the layout of a bucket shouldn't matter too
   much. However, for the interested reader: there are two areas, the
   sorted area and the unsorted area. New kvs are added to the
   unsorted area. When this is full, the unsorted kvs are merged with
   the sorted kvs. Searching for a key proceeds first to scan the
   unsorted area, then the sorted area (using interpolation search for
   the sorted area). The idea of this design is to make insertion of a
   new kv fast on average, whilst also keeping most of the keys sorted
   (to allow quick search). The exact size of the unsorted area and
   the sorted area is probably best determined by experiment.

In order to implement this approach, we need to track (within the
   bucket) the length of the sorted region and the length of the
   unsorted region. This requires two ints. So we have 510 ints
   available for the sorted and unsorted kvs. Since a kv is a pair of
   ints, we can store 255 kvs in each bucket (at most). Currently we
   choose (rather arbitrarily) to store max 10 unsorted, and 245
   sorted kvs in each bucket.
 *)

open Util

(** Bucket configuration: we can choose how many sorted elements, and
   how many unsorted, in order to get the best performance *)
module type BUCKET_CONFIG = sig
  val max_sorted            : int  
  val max_unsorted          : int
  val bucket_length_in_ints : int 
  (** Length of backing int bigarray, measured in ints; must be large
      enough to include 2*(max_sorted+max_unsorted)+2 *)
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

  type t
  type bucket = t

  type k := int
  type v := int

  (** NOTE The backing int bigarray is not controlled by the bucket;
     so we have functions to convert between a bucket and a bigarray,
     and a way to initialize a new bigarray *)

  (** Return the int bigarray underlying the bucket *)
  val to_bigarray : bucket -> int_bigarray (* guaranteed to be of size bucket_length_in_ints *)

  (** Construct a bucket from a bigarray; if the bigarray is new, we
     will need to initialize it using [init_sorted] *)
  val of_bigarray : int_bigarray -> bucket

  (** Initialize a bucket FIXME the list must fit in the backing array *)
  val init_sorted : bucket -> (k*v)list -> unit

  val find        : bucket -> k -> v option
  val insert      : bucket -> k -> v -> [ `Ok | `Split of (k*v)list * k * (k*v)list ]

  val show        : bucket -> unit
  val export      : bucket -> exported_bucket

  (* This adds debugging for each operation; expensive! *)
  module With_debug() : sig
    val find   : bucket -> k -> v option
    val insert : bucket -> k -> v -> [ `Ok | `Split of (k*v)list * k * (k*v)list ]
  end

end
