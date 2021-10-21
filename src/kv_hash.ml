(** Kv-hash, a key-value store *)


(** {2 Bucket} *)

module Bucket_intf = Bucket_intf
module Bucket = Bucket
module Bucket0 = Bucket.Bucket0


(** {2 Bucket store} *)

module Bucket_store_intf = Bucket_store_intf
module Bucket_store = Bucket_store
module Bucket_store0 = Bucket_store.Bucket_store0


(** {2 Partition} *)

module Partition_intf = Partition_intf
module Partition = Partition


(** {2 Non-volatile map (int -> int) } *)

(** Bucket freelist *)
module Freelist = Freelist

module Nv_map_ii_intf = Nv_map_ii_intf
module Nv_map_ii = Nv_map_ii
module Nv_map_ii0 = Nv_map_ii.Nv_map_ii0


(** {2 Non-volatile map (string -> string) } *)

module Values_file = Values_file
module Nv_map_ss_intf = Nv_map_ss_intf
module Nv_map_ss_private = Nv_map_ss_private
module Nv_map_ss0 = Nv_map_ss_private.Nv_map_ss0


(** {2 Frontend and merge process} *)

module Frontend = Frontend
module Merge_process = Merge_process


module Private = struct
  module Config=Config
  module Util=Util
end
