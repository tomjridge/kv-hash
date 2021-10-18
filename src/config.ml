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

end
open Consts

let debug_sysenv = Sys.getenv_opt "KVHASH_DEBUG"
let debug = debug_sysenv <> None

type config = {
  initial_bucket_store_size    : int; (* in bytes *)
  initial_number_of_partitions : int;
  max_log_length               : int; (* in bytes *)
  lru_capacity                 : int; (* in kvs *)
  debug_bucket                 : bool;
  blk_sz                       : int;
  bucket_sorted                : int;
  bucket_unsorted              : int;
  ctl_fn                       : string;
  values_fn                    : string;
  bucket_store_fn              : string;
}[@@deriving sexp]

let default_config = { 
  initial_bucket_store_size    = const_4GB;
  initial_number_of_partitions = 100_000; (* 10M for stress testing *)
  max_log_length               = 128 * const_1MiB;
  lru_capacity                 = 100_000;
  debug_bucket                 = false;
  blk_sz                       = 4096;
  bucket_sorted                = 245;
  bucket_unsorted              = 10;
  ctl_fn                       = "ctl.data";
  values_fn                    = "values.data";
  bucket_store_fn              = "buckets.data";  
}

let read ~fn =
  Stdio.In_channel.(
    create fn |> fun ic -> 
    input_all ic |> fun s -> 
    close ic;
    config_of_sexp (sexp_of_string s))

let write t ~fn =
  Stdio.Out_channel.(
    create fn |> fun oc -> 
    sexp_of_config t |> Sexplib0.Sexp.to_string_hum |> fun s -> 
    output_string oc s;
    close_no_err oc;
    ())

  
let config_sysenv = Sys.getenv_opt "KVHASH_CONFIG"
let config = 
  match config_sysenv with
  | None -> default_config
  | Some fn -> 
    try
      read ~fn
    with e -> 
      Printf.printf "%s: could not read config file %s\n%!Error: %s" 
        __MODULE__
        fn
        (Printexc.to_string_default e);
      Printf.printf "For reference, a config file should contain something like: %s\n%!"
        (default_config |> sexp_of_config |> Sexplib0.Sexp.to_string_hum);
      Stdlib.exit (-1)

