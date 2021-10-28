(** Read-only frontend *)


(** In addition to the main frontend, which writes logs and calls the
   merge process, we also have read-only processes that read the logs
   directly from disk, and also use the on-disk partition to access the
   bucket store.

Steps to create:
- open the control file, read the current log number
- open the previous (if any) and current log
- from the control file, read the current partition; load partition


To service a request:
- the RO process needs to keep itself up to date if not serving requests
  - reinit from prev_log, current_log 
  - reinit from current partition
- service request
- check partition; if changed, retry

FIXME we need to decide how to test this rigorously.

FIXME this code feels a bit trickier than it should be
 *)

open Util
open Frontend

(* open Frontend.KV *)
open Frontend.Op

open struct
  module Bucket_store0 = Bucket_store.Bucket_store0
end

module Log_file_r = struct

  (* FIXME we need to think about what to do if a log file becomes
     corrupted and cannot be read in full *)

  open KV

  type t = {
    ic: Stdlib.in_channel;
    tbl: (k,[`Insert of v | `Delete])Hashtbl.t;
  }

  (* soft limit *)
  let can_read t = pos_in t.ic < (Stdlib.in_channel_length t.ic)
                                 
  let rec read t : op = 
    assert(can_read t);
    let pos = pos_in t.ic in
    try 
      input_value t.ic
    with Sys_error _e -> 
      warn(fun () -> Printf.sprintf "%s: Log_file_r read a partial value\n" __LOC__);
      (* We read a partial value; try again *)
      (* check that we haven't somehow moved the position in the ic *)
      assert(pos_in t.ic = pos); 
      (* FIXME perhaps if the position has moved we should reset, rather than fail? *)
      read t 

  let sync t = 
    while (can_read t) do
      read t |> fun (k,v') -> 
      Hashtbl.replace t.tbl k v';
      ()
    done

  let open_ ?soft_max_entries:(soft_max_entries=1024) ~fn () = 
    let t = {
      ic=open_in_bin fn;
      tbl=Hashtbl.create soft_max_entries;
    }
    in
    sync t;
    t
    
  let close t = close_in_noerr t.ic

end

module Reader1 = struct

  (* The initial task of the reader is to get itself in sync with the writer *)

  type log = Log_file_r.t    

  type t = {
    ctl       : Control.t;

    logs      : (int,log)Hashtbl.t;
    (** The int key corresponds to the filename of the file that backs
       the relevant log *)

    mutable part_n : int;  
    (** The int corresponding to the file the partition was loaded
       from; not necessarily the current partition *)

    partition : Partition_ii.t;
    buckets   : Bucket_store0.t;
    values    : Values_file.t
  }

  let max_initial_wait = 60 (* seconds *)

  (* When syncing, the general pattern is:
     - read ctl field to get integer value n
     - try and perform action, assuming n is current
       - if success, we are done
       - if failure, it may be that n changed (is out of date), in which case we need to redo the action

     In general: we read an integer, then do various actions; at the
     end, we re-read the integer; if it hasn't changed then we are
     finished, otherwise we need to redo the actions.

     FIXME perhaps abstract this control flow and see if the code is any clearer; with_constant ~counter (f: int -> [`Ok | `Error]); if `Error, but counter changed, we retry
  *)

  let with_constant ~(counter:unit -> int) (f:int -> 'a) =
    () |> iter_k (fun ~k:retry () -> 
        let c = counter () in
        f c |> fun r -> 
        match c = counter() with
        | true -> r
        | false -> retry ())

  let _ : counter:(unit -> int) -> (int -> 'a) -> 'a = with_constant

  let sync_logs ctl logs =
    let curr_log_n () = Control.(get_field ctl F.current_log) in
    let sync_log n = 
      match Hashtbl.find_opt logs n with
      | Some log -> (Log_file_r.sync log; `Ok)
      | None -> 
        let fn = log_fn n in
        try
          let log = Log_file_r.open_ ~fn () in
          Hashtbl.replace logs n log;
          `Ok
        with _e -> 
          `Error
    in
    (* NOTE the continuation is named "retry" *)
    let `Finished n = 
      () |> iter_k (fun ~k:retry () -> 
          let n = curr_log_n () in
          let n_changed () = curr_log_n () <> n in
          sync_log n |> function 
          | `Ok -> (
              assert(generation_starts_at_1);
              (* try to load the previous log, if any *)
              match n with 
              | 1 -> `Finished n
              | _ -> 
                sync_log (n-1) |> function
                | `Ok -> `Finished n
                | `Error -> 
                  (* retry? *)
                  match n_changed() with
                  | false -> failwith (
                      Printf.sprintf "Fatal: could not load previous log \
                                      from file %s\n%!" (log_fn (n-1)))
                  | true -> retry ())
          | `Error -> 
            (* retry? *)
            match n_changed() with
            | false -> failwith (
                Printf.sprintf "Fatal: could not load current log \
                                from file %s\n%!" (log_fn n))
            | true -> retry ())
    in
    (* remove all but the curr and prev logs *)
    logs |> Hashtbl.filter_map_inplace 
      (fun k v -> if k=n || k=n-1 then Some v else (Log_file_r.close v; None));
    (* at this point, the logs are synced to some consistent point
       between the start of the sync_logs function and the end *)
    ()

  (* FIXME this will reload the partition every time, but it should
     only do so if part_n has changed *)
  let sync_partition t = 
    let curr_part_n () = Control.(get_field t.ctl F.partition) in
    () |> iter_k (fun ~k:retry () ->           
        let n = curr_part_n () in
        let n_changed () = n <> curr_part_n () in
        let fn = part_fn n in        
        try
          Partition_ii.reload t.partition ~fn;
          t.part_n <- n
        with Sys_error _e -> (
            (* assume file missing *)
            match n_changed() with
            | false -> 
              Printf.sprintf "Fatal: control file exists, but could \
                              not read partition from file %s\n%!" fn |> failwith
            | true -> retry () ));
    ()

  let sync t = 
    let ctl = t.ctl in
    let get_m () = Control.(get_field ctl F.current_log) in
    let get_n () = Control.(get_field ctl F.partition) in
    () |> iter_k (fun ~k:retry () -> 
        let m = get_m () in
        let n = get_n () in
        sync_logs t.ctl t.logs;
        sync_partition t;
        (* NOTE the order of getting m and n is reversed here; the
           point is: if nothing has changed we are sure there is some
           point where m and n were valid *)
        let n' = get_n () in
        let m' = get_m () in
        match (m,n) = (m',n') with
        | true -> ()
        | false -> retry ())

  let open_ 
      ?ctl_fn:(ctl_fn=Config.config.ctl_fn)
      ?buckets_fn:(buckets_fn=Config.config.bucket_store_fn)
      ?values_fn:(values_fn=Config.config.values_fn)
      ()
    =
    (* NOTE existence of ctl is taken to indicate that the other
       relevant files exist too; so we wait for ctl to exist before
       doing anything *)
    let ctl = 
      (* Load control; if it doesn't exist, wait till it does *)
      0 |> iter_k (fun ~k:kont n -> 
          Sys.file_exists ctl_fn |> function
          | false -> (
              Thread.yield (); 
              (if n mod 1_000_000_000 = 0 then 
                 Printf.printf "WARNING: %s: read-only thread waited \
                                for 1B iterations for control file\n%!" __MODULE__);
              kont (n+1))
          | true -> 
            Control.(open_ ~fn:ctl_fn))
    in
    (* NOTE from the point we read the current partition number from
       control, to the time we try to read the partition file itself,
       the partition number may change and the partition we are
       looking for may be deleted; so if something goes wrong we check
       the control again and retry if something has changed *)
    let part_n,partition =    
      let curr_part_n () = Control.(get_field ctl F.partition) in
      () |> iter_k (fun ~k:retry () ->           
          let n = curr_part_n () in
          let n_changed () = n <> curr_part_n () in
          let fn = part_fn n in
          try
            n,Partition_ii.read_fn ~fn 
          with Sys_error _e -> (
              (* assume file missing *)
              match n_changed() with
              | false -> 
                Printf.sprintf "Fatal: control file exists, but could \
                                not read partition from file %s\n%!" fn |> failwith
              | true -> retry () ))
    in
    let buckets = Bucket_store0.open_ ~fn:buckets_fn in
    let values = Values_file.open_ ~fn:values_fn in
    (* logs *)
    let logs = Hashtbl.create 3 in
    sync_logs ctl logs;
    { ctl; logs; part_n; partition; buckets; values }

(*  
  (* FIXME to implement this we really need the curr_log and prev log in the state *)
  let find' t k = 
    let map = function `Insert v -> Some v | `Delete -> None in
    let n = 
    Hashtbl.find_opt 

  
  let find t k = 
    with_constant 
      ~counter:(fun () -> Control.(get_field t.ctl F.partition))
      (fun _ -> 
         sync_partition t;
         with_constant 
           ~counter:(fun () -> Control.(get_field t.ctl F.current_log))
           (fun _ -> 
              sync_logs t.ctl t.logs;
              find' t k))
*)              
    


end


