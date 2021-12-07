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

Following pic taken from Documentation.md:


----> time

0        1       2       3       4       5       6       7       8       9       10
+--------+-------+-------+-------+-------+-------+-------+-------+-------+-------+
         log(1)          log(2)          log(3)          log(4)
                         m(1)----X       m(2)----X       m(3)----X
part(0)----------------------------------X
                                 part(1)-----------------X
                                                 part(2)-----------------X
                                                                 part(3)-----------------X


 *)

open Util

(* NOTE this brings in a lot of modules, like Nv_map_ss_ *)
open Frontend


(* open Frontend.KV *)
open Frontend.Op

open struct
  module Bucket_store0 = Bucket_store.Bucket_store0
end

module Log_file_r = struct

  (* FIXME we need to think about what to do if a log file becomes
     corrupted and cannot be read in full *)

  open Frontend.KV

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

  let find_opt t k = Hashtbl.find_opt t.tbl k |> function
    | None -> None
    | Some `Delete -> None
    | Some (`Insert v) -> Some v

end

module Reader1 = struct

  (* The initial task of the reader is to get itself in sync with the writer *)

  type log = Log_file_r.t    

  type t = {
    ctl       : Control.t;

    logs      : (int,log)Hashtbl.t;
    (** The int key corresponds to the filename of the file that backs
       the relevant log *)

    mutable curr_log: int;
    (** The current log the last time the logs were synced; may lag
       the control file *)
    

    mutable part_n : int;  
    (** The int corresponding to the file the partition was loaded
       from; not necessarily the current partition *)

    nv_map_ss   : Nv_map_ss_.t;
    (** nv_map_ss must be updated when the partition changes *)

  }

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

  let get_partition t : Partition_ii.t = 
    t.nv_map_ss |> Nv_map_ss_.get_nv_map_ii |> Nv_map_ii_.get_partition

  let with_constant ~(counter:unit -> int) (f:int -> 'a) =
    () |> iter_k (fun ~k:retry () -> 
        let c = counter () in
        f c |> fun r -> 
        match c = counter() with
        | true -> r
        | false -> retry ())

  let _ : counter:(unit -> int) -> (int -> 'a) -> 'a = with_constant

  (** Sync the logs; after calling, the logs are guaranteed to be
     synced to some point between when the function started and when
     it ended *)
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
    n

  let sync_logs t = 
    sync_logs t.ctl t.logs |> fun n -> 
    t.curr_log <- n
    
  (* FIXME this will reload the partition every time, but really it
     should only do so if part_n has changed *)
  (** Sync the partition; after calling, the partition is guaranteed
     current for some point during the call *)
  let sync_partition t = 
    let curr_part_n () = Control.(get_field t.ctl F.partition) in
    () |> iter_k (fun ~k:retry () ->           
        let n = curr_part_n () in
        let n_changed () = n <> curr_part_n () in
        let fn = part_fn n in        
        try
          Partition_ii.reload (get_partition t) ~fn;
          t.part_n <- n;
        with Sys_error _e -> (
            (* assume file missing *)
            match n_changed() with
            | false -> 
              Printf.sprintf "Fatal: control file exists, but could \
                              not read partition from file %s\n%!" fn |> failwith
            | true -> retry () ));
    ()

  (** Sync to the latest log and partition *)
  let sync t = 
    let ctl = t.ctl in
    (* FIXME we should retry when log changes only; and part(n) is
       only invalid by log(n+3); we want to know that there is a
       single point in time when the log and the partition were
       current; this occurs if partition is unchanged over a period
       and we read the log; so we should retry only if partition
       changes *)
    let get_m () = Control.(get_field ctl F.current_log) in
    let get_n () = Control.(get_field ctl F.partition) in
    () |> iter_k (fun ~k:retry () -> 
        let m = get_m () in
        let n = get_n () in
        sync_logs t;
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
      ()
    =
    (* NOTE existence of ctl is taken to indicate that the other
       relevant files exist too; so we wait for ctl to exist before
       doing anything *)
    let ctl =       
      (* Load control; if it doesn't exist, wait till it does *)
      Util.wait_for_file_to_exist ~fn:ctl_fn;
      Control.(open_ ~fn:ctl_fn)
    in
    let values = Values_file.open_ ~fn:values_fn in
    (* NOTE from the point we read the current partition number from
       control, to the time we try to read the partition file itself,
       the partition number may change and the partition we are
       looking for may be deleted; so if something goes wrong we check
       the control again and retry if something has changed *)
    (* nv_map_ii *)
    let part_n,nv_map_ii =    
      let curr_part_n () = Control.(get_field ctl F.partition) in
      () |> iter_k (fun ~k:retry () ->           
          let n = curr_part_n () in
          let n_changed () = n <> curr_part_n () in
          let fn = part_fn n in
          try
            (* FIXME make sure Nv_map_ii_.open_ro doesn't leak fds *)
            n,Nv_map_ii_.open_ro ~buckets_fn ~partition_fn:fn              
          with Sys_error _e -> (
              (* assume file missing *)
              match n_changed() with
              | false -> 
                Printf.sprintf "Fatal: control file exists, but could \
                                not read partition from file %s\n%!" fn |> failwith
              | true -> retry () ))
    in
    (* nv_map_ss *)
    let nv_map_ss = Nv_map_ss_.create values nv_map_ii in    
    (* logs *)
    let logs = Hashtbl.create 3 in
    let t = { ctl; logs; curr_log=0; part_n; nv_map_ss; } in    
    sync_logs t;
    assert(t.curr_log > 0);
    t

  let close t = 
    (* Control.close t.ctl; FIXME add *)
    Nv_map_ss_.close t.nv_map_ss;
    t.logs |> Hashtbl.iter (fun _ log -> Log_file_r.close log);
    Hashtbl.clear t.logs;
    ()
        
  let find' t k = 
    let curr_log_n = t.curr_log in
    let curr_log = 
      (* invariant: t.logs contains t.curr_log *)
      Hashtbl.find t.logs curr_log_n in
    (* check curr_log *)
    Log_file_r.find_opt curr_log k |> function
    | Some v -> Some v
    | None -> 
      begin  (* check prev_log *)
        Hashtbl.find_opt t.logs (curr_log_n - 1) |> function
        | None -> None
        | Some prev_log -> 
          Log_file_r.find_opt prev_log k |> function
          | Some v -> Some v
          | None -> None
      end |> function
      | Some v -> Some v
      | None -> 
        (* now go to the main store *)
        Nv_map_ss_.find_opt t.nv_map_ss k
        
  (** NOTE find requires that partition doesn't change (otherwise it will just retry); then it syncs the partition and the logs and calls find *)
  let find t k = 
    with_constant 
      ~counter:(fun () -> Control.(get_field t.ctl F.partition))
      (fun _ -> 
         sync t;
         find' t k)

end


