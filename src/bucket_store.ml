(** A store of buckets, backed by a file/mmap *)

open Util
open Tjr_mmap (* FIXME hide tjr_mmap util *)
open Bucket_intf
open Bucket_store_intf

(* FIXME add debugging back in - see old *)

(** Version with mmap *)
module Make_with_mmap(Raw_bucket: BUCKET) = struct
  type raw_bucket = Raw_bucket.t

  type t = {
    fn             : string;
    fd             : Unix.file_descr;
    mmap           : Mmap.int_mmap;
    mutable closed : bool;
  }

  type bucket = {
    index:int;
    raw_bucket: Raw_bucket.t
  }

  let create ?sz:(sz=1024) ~fn () =
    Unix.(openfile fn [O_CREAT;O_TRUNC;O_RDWR] perm0) |> fun fd -> 
    Unix.(ftruncate fd (sz*Config.config.blk_sz));
    let mmap = Mmap.(of_fd fd int_kind) in
    { fn; fd; mmap; closed=false }

  let open_ ~fn = 
    Unix.(openfile fn [O_CREAT;O_RDWR] perm0) |> fun fd -> 
    let mmap = Mmap.(of_fd fd int_kind) in
    { fn; fd; mmap; closed=false }

  let read_bucket t i = 
    assert(not t.closed);
    Mmap.sub t.mmap ~off:(i*Config.config.blk_sz) ~len:Raw_bucket.bucket_length_in_ints |> fun arr -> 
    Raw_bucket.of_bigarray arr |> fun raw_bucket -> 
    { index=i; raw_bucket }

  let write_bucket _t _b =     
    (* raw_bucket is backed by an mmap'ed array, so no need to do anything here *)
    ()

  let sync t = 
    assert(not t.closed);
    Mmap.msync t.mmap;
    ()

  let close t = 
    assert(not t.closed);
    sync t;
    Mmap.close t.mmap; (* closes the underlying fd *)
    t.closed <- true;
    ()

end

module Private_mmap = struct
  module Make(Raw_bucket:BUCKET) : BUCKET_STORE with type raw_bucket=Raw_bucket.t 
    = Make_with_mmap(Raw_bucket)

  (** Standard instance; other instances for testing *)
  module Bucket_store0 = Make_with_mmap(Bucket.Bucket0)
end

module Make_with_fd(Raw_bucket:BUCKET) = struct
  type raw_bucket = Raw_bucket.t

  type t = {
    fn             : string;
    fd             : Unix.file_descr;
    mutable closed : bool;
  }

  type bucket = {
    index:int;
    raw_bucket: Raw_bucket.t
  }

  let create ?sz:(sz=1024) ~fn () =
    Unix.(openfile fn [O_CREAT;O_TRUNC;O_RDWR;O_NONBLOCK] perm0) |> fun fd -> 
    Unix.(ftruncate fd (sz*Config.config.blk_sz));
    { fn; fd; closed=false }

  let open_ ~fn = 
    Unix.(openfile fn [O_CREAT;O_RDWR;O_NONBLOCK] perm0) |> fun fd -> 
    { fn; fd; closed=false }

  let blk_sz = Config.config.blk_sz

  let read_bucket t i = 
    assert(not t.closed);
    let off = i*blk_sz in
    read_int_ba ~blk_sz ~fd:t.fd ~off |> fun arr ->
    Raw_bucket.of_bigarray arr |> fun raw_bucket -> 
    { index=i; raw_bucket }

  let write_bucket t b =     
    assert(not t.closed);
    let off = b.index * blk_sz in
    write_int_ba ~fd:t.fd ~off (Raw_bucket.to_bigarray b.raw_bucket);
    ()

  let sync t = 
    assert(not t.closed);
    Unix.fsync t.fd;
    ()

  let close t = 
    assert(not t.closed);
    sync t;
    (* Mmap.close t.mmap; (\* closes the underlying fd *\) *)
    t.closed <- true;
    ()

end


module Make(Raw_bucket:BUCKET) : BUCKET_STORE with type raw_bucket=Raw_bucket.t 
  = Make_with_fd(Raw_bucket)

(** Standard instance; other instances for testing *)
module Bucket_store0 = Make_with_fd(Bucket.Bucket0)
