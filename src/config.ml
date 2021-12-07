(** Various configuration values, in [Config.config]

Envvars:
- KVHASH_DEBUG - enable debugging code
- KVHASH_CONFIG - if set, the filename of the config options, in sexp format

*)

open Sexplib.Std

module Consts = struct

  let const_4GB = 4_294967296

  let const_1GiB = 1_073_741_824

  let const_1k = 1024

  let const_1M = 1_000_000

  let const_1MiB = const_1k * const_1k

  let library_name = "kv-hash"

  let ctl_fn                       = "ctl.data"
  let values_fn                    = "values.data"
  let buckets_fn                   = "buckets.data"
  let partition_fn                 = "partition.data"
  (* let freelist_fn                  = "freelist.data" *)

end
open Consts

let debug_sysenv = Sys.getenv_opt "KVHASH_DEBUG"
let debug = debug_sysenv <> None

module T = struct
  type config = {
    initial_bucket_store_size    : int; (* in bytes *)
    initial_number_of_partitions : int;
    max_log_length               : int; (* in bytes *)
    lru_capacity                 : int; (* in kvs *)
    debug_bucket                 : bool; (* FIXME use env var *)
    blk_sz                       : int;
    bucket_sorted                : int;
    bucket_unsorted              : int;
  }[@@deriving sexp]
end
include T

let default_config = { 
  initial_bucket_store_size    = const_4GB;
  initial_number_of_partitions = 100_000; (* 10M for stress testing *)
  max_log_length               = 128 * const_1MiB;
  lru_capacity                 = 100_000;
  debug_bucket                 = false;
  blk_sz                       = 4096;
  bucket_sorted                = 245;
  bucket_unsorted              = 10;
}

let read ~fn = Sexplib.Sexp.load_sexp fn |> config_of_sexp

let write t ~fn = t |> sexp_of_config |> Sexplib.Sexp.save_hum fn
  
let config_sysenv = Sys.getenv_opt "KVHASH_CONFIG"
let config = 
  match config_sysenv with
  | None -> default_config
  | Some fn -> 
    try
      read ~fn
    with e -> 
      Printf.printf "%s: could not interpret config file %s\n%!Error: %s\n%!" 
        __MODULE__
        fn
        (Printexc.to_string_default e);
      Printf.printf "For reference, a config file should contain something like:\n%s\n%!"
        (default_config |> sexp_of_config |> Sexplib.Sexp.to_string_hum);
      Stdlib.exit (-1)


let test ~fn =
  let t = default_config in
  t |> sexp_of_config |> Sexplib.Sexp.to_string_hum |> print_endline;
  write t ~fn;
  let t' = read ~fn in
  assert(t=t');
  ()


