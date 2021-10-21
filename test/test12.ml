(** Test config file format *)

open Kv_hash

let _ = 
  Printf.printf "%s: testing write/read of config\n%!" __MODULE__;
  Private.Config.test ~fn:"KVHASH_CONFIG_TEST";
  ()
