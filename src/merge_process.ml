(** A process that is responsible for merging data into the main KV store. 

Readers can still access the store whilst the merge is in process. 

The merge process is forked from the frontend. It has access to the
log entries (as a hashtable) and the "current" partition. During the
merge, the partition is expected to change. Thus, there needs to be a
way for the merge process to communicate the updated partition back to
the frontend. Moreover, the partition needs to be shared with any RO
processes. The simplest way to do this is just to serialize the partition to a file "partition_1234", where the "1234" is the generation number. Then change the generation number in the control block, and delete the old generation.

*)
(* open Util *)

[@@@warning "-32"]


(** Operations are insert: (k,`Insert v), or delete: (k,`Delete) *)
type op = string * [ `Insert of string | `Delete ]

module Make(Partition: Partition_intf.PARTITION with type k = int and type r = int) = struct

  [@@@warning "-26-27"]

  (* perform the merge; if partition has changed, write to file
     "partition_1234" (with 1234 replaced by the next generation
     number), and call set_generation *)
  let merge_and_exit 
      ~generation 
      ~set_generation 
      ~(partition:Partition.t) 
      ~(ops:op list) 
      ~(batch:op list -> unit)
    = 
    let partition_changed = ref false in
    Partition.set_split_hook partition (fun () -> partition_changed := true);
    batch ops;
    begin
      match !partition_changed with
      | false -> ()
      | true -> 
        let generation = generation + 1 in
        let fn = "partition_"^(string_of_int generation) in
        let oc = open_out fn in
        Partition.write partition oc;
        close_out_noerr oc;
        set_generation generation;
        ()
    end;    
    Stdlib.exit 0

end
