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

module Make(S:sig 
    val generation: int

    module Partition : Partition_intf.PARTITION with type k = int and type r = int
    val partition: Partition.t
  end) = struct

  

end
