(*
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
*)
