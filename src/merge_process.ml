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


(** Operations are insert: (k,`Insert v), or delete: (k,`Delete) *)
type op = string * [ `Insert of string | `Delete ]

module Partition_ = Partition.Partition_ii

include struct

  (* [@@@warning "-26-27"] *)

  (* perform the merge; call post_merge_hook; if partition has
     changed, write to file "partition_1234" (with 1234 replaced by
     the nonce) and call partition_change_hook *)
  (* NOTE ops do not need to be sorted - that happens in pmap.batch *)
  let merge_and_exit 
      ~merge_nonce 
      ~post_merge_hook
      ~partition_nonce
      ~partition_change_hook (* if there are changes to the partition *)
      ~(pmap:String_string_map.t) 
      ~(ops:op list)
    = 
    let partition_changed = ref false in
    let partition = 
      (* FIXME rename Pmap_ss *)
      String_string_map.get_phash pmap |> 
      String_string_map_private.Make_1.Phash.get_partition      
    in
    Partition_.set_split_hook partition (fun () -> partition_changed := true);
    String_string_map.batch pmap ops;
    post_merge_hook merge_nonce;
    begin
      match !partition_changed with
      | false -> ()
      | true -> 
        let fn = Util.part_fn partition_nonce in
        let oc = open_out fn in
        Partition_.write partition oc;
        close_out_noerr oc;
        partition_change_hook partition_nonce;
        ()
    end;    
    Stdlib.exit 0

end

