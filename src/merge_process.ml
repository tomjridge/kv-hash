(** A process that is responsible for merging data into the main KV
   store.

Readers can still access the store whilst the merge is in process.

The merge process is forked from the frontend. It has access to the
   log entries (as a hashtable) and the "current" partition. During
   the merge, the partition may change. Thus, there needs to be a way
   for the merge process to communicate the updated partition back to
   the frontend. Moreover, the partition needs to be shared with any
   RO processes. The simplest way to do this is just to serialize the
   partition to a file "partition_1234", where the "1234" is some
   nonce number. Then change the number in the control block, and
   delete the old file.

*)

open Util

(** Operations are insert: (k,`Insert v), or delete: (k,`Delete) *)
type op = string * [ `Insert of string | `Delete ]

module Partition_ = Partition.Partition_ii

module Make(Nv_map_ss:Nv_map_ss_private.S) = struct

  (* perform the merge; call post_merge_hook; if partition has
     changed, write to file "partition_1234" (with 1234 replaced by
     the nonce) and call partition_change_hook *)
  (* NOTE ops do not need to be sorted - that happens in pmap.batch *)
  let merge_and_exit
      ~(gen:int)
      ~(pmap:Nv_map_ss.t) 
      ~(ops:op list)
    = 
    let t1 = Unix.time () in
    warn (fun () -> Printf.sprintf "Merge started\n%!");
    let partition = 
      (* FIXME rename Pmap_ss *)
      Nv_map_ss.get_phash pmap |> 
      Nv_map_ss_private.Make_1.Phash.get_partition
    in
    let len1 = Partition_.length partition in
    Nv_map_ss.batch pmap ops;
    let len2 = Partition_.length partition in
    begin
      match len1 = len2 with
      | true -> 
        (* no need to write out partition *)
        warn(fun () -> "Merge_process: partition was unchanged");
        ()
      | false -> 
        (* partition changed; use gen as the new partition filename *)
        warn(fun () -> "Merge_process: partition changed");
        let fn = Util.part_fn gen in
        let oc = open_out fn in
        Partition_.write partition oc;
        close_out_noerr oc;
        ()
    end;    
    let t2 = Unix.time () in
    warn (fun () -> Printf.sprintf "Merge process terminated in %f\n%!" (t2 -. t1));
    Stdlib.exit 0

end

