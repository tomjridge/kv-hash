(** Logging, using Buenzli's logs library *)


let src = Logs.Src.create "kv-hash library" ~doc:"kv-hash library logging src"

module Log = (val Logs.src_log src : Logs.LOG)

include Log
