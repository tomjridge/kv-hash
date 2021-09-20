(** A process that is responsible for merging data into the main KV store. 

Readers can still access the store whilst the merge is in process. 

The merge process is forked from the frontend. It has access to the log entries (as a hashtable) and the "current" partition. During the merge, the partition is expected to change. Thus, there needs to be a way for the merge process to communicate the updated partition back to the frontend. Moreover, the partition needs to be shared with any RO processes. 

*)
open Util

type ('k,'v) op = 'k * [ `Insert of 'v | `Delete ]

module type BACKEND = sig

  type t

  type k
  type v

  type partition

  (** Returns the partition and the generation number of the partition
     (generation gets bumped on modifications to partition) *)
  val get_partition : t -> partition * int
    
  val batch : t -> ('k,'v) op list -> unit

end

type config = Frontend.config

module Control = Frontend.Control
open Control

type t = Frontend.From_config.t
open Frontend.From_config

module Make = struct  

  let create ~config = Frontend.From_config.create ~config 

     
  (**

The merge process does the following:

- check control block for "want_merge"; this is the generation of the
     log that we want merging; it is 0 to indicate "no merge"
- "last_merged" is the generation of the log that we last merged
- if we have already merged "want_merge", then just loop
- otherwise, want_merge should correspond to the non-active log
- load the log, and execute as a batch on the backend
- update the "last_merged" data, and loop again

*)
  let start t = 
    (* wait for merge to be enabled *)
    begin 
      () |> iter_k (fun ~k () -> 
          match t.ctl_buf.{merge_enabled} > 0 with
          | false -> (Thread.yield(); k())
          | true -> ()) 
    end |> fun () -> 
    let last_merged = 0 in
    begin
      last_merged |> iter_k (fun ~k:kont last_merged -> 

    let rec loop last_merged = 
      let ctl = Control.read t.ctl_buf in
      match ctl.active_gen = last_merged+1 with
      | true -> (Thread.yield(); loop t last_merged)
      | false -> 
        assert(ctl.active_gen = last_merged+2);
        let merge_log = 1 - ctl.active_log in
        let ops = 
          
    in 
          

end
