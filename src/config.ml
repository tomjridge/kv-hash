(** Various configuration values *)

open Sexplib.Std

module Consts = struct

  let const_4GB = 4_294967296

  let const_1GiB = 1_073_741_824

  let const_1k = 1024

  let const_1M = 1_000_000

  let const_1MiB = const_1k * const_1k

end
open Consts

let debug_sysenv = Sys.getenv_opt "DEBUG_KVHASH"
let debug = debug_sysenv <> None

type t = {
  initial_bucket_store_size    : int; (* in bytes *)
  initial_number_of_partitions : int;
  max_log_length               : int; (* in bytes *)
  lru_capacity                 : int; (* in kvs *)
}[@@deriving sexp]

let default_config = { 
  initial_bucket_store_size    = const_4GB;
  initial_number_of_partitions = 10_000;
  max_log_length               = 128 * const_1MiB;
  lru_capacity                 = 10_000;
}

