(** A store of buckets, backed by a file/mmap *)

open Util
(* open Tjr_mmap (\* FIXME hide tjr_mmap util *\) *)
open Bucket_intf
open Bucket_store_intf

(* FIXME add debugging back in - see old *)

(** NOTE Version with mmap is deprecated and moved to a separate file *)

(** Version with file descriptor *)
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

  (* We let the OS know we want to access the file randomly, with no
     caching if possible; FIXME should ideally use *)
  let fadvise_ fd = 
    (* if len is 0, it means "the whole file" on Linux *)
    ExtUnix.Specific.(fadvise fd 0 0(*len*) POSIX_FADV_RANDOM);
    ExtUnix.Specific.(fadvise fd 0 0(*len*) POSIX_FADV_DONTNEED);
    (* POSIX_FADV_NOREUSE also an option, but strictly speaking it
       doesn't fit our usecase *)
    ()    

  let create ?sz:(sz=1024) ~fn () =
    Unix.(openfile fn [O_CREAT;O_TRUNC;O_RDWR;O_NONBLOCK] perm0) |> fun fd -> 
    Unix.(ftruncate fd (sz*Config.config.blk_sz));
    fadvise_ fd;
    { fn; fd; closed=false }

  let open_ ~fn = 
    Unix.(openfile fn [O_CREAT;O_RDWR;O_NONBLOCK] perm0) |> fun fd -> 
    fadvise_ fd;
    { fn; fd; closed=false }

  (* FIXME treat blk_sz consistently here and elsewhere; perhaps best
     to take from config file as we do here, rather than parameterize
     everywhere *)
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
    Unix.close t.fd;
    t.closed <- true;
    ()

end


module Make(Raw_bucket:BUCKET) : BUCKET_STORE with type raw_bucket=Raw_bucket.t 
  = Make_with_fd(Raw_bucket)

(** Standard instance; other instances for testing *)
module Bucket_store0 = Make_with_fd(Bucket.Bucket0)
