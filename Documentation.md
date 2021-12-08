# Kv-hash documentation

This supplements the documentation in the code itself.

Kv-hash is a library which implements a key-value store. Keys and values can be arbitrary strings. 

For the individual components -- buckets and partitions -- we work with the int hash of a given key (so, an int key corresponding to some actual string key) and an int value (the offset of the string value in some "values file").

The main idea is to maintain a "partition" in memory that describes how to map an (int) key to a particular on-disk bucket. There are many buckets, each corresponding to a distinct part of the key space, i.e., for bucket $i$, and int key $k$ in that bucket, we have  $l_i \le k < h_i$ for some $l_i, h_i$, and the ranges are disjoint and span the entire int key space ($0... \textit{max_int}$). We can then add a new kv by adding to the corresponding bucket, or find the value corresponding to a key by looking in the corresponding bucket.

There are several concepts we need to describe:

* converting keys to ints; converting values to ints; the "values file"
* bucket 
* bucket store
* partition
* non-volatile map (int -> int)
* non-volatile map (string -> string)
* frontend log rotation
* control file

## Architecture

The architecture is a simple layered architecture. Effectively, each layer talks only to the layer immediately below it, but we break this to allow for the frontend to retrieve the last flushed state of various components in lower layers.

```
                                                                           
                                                                              
  +-------------------------------------------+                               
  | Frontend (layer 4)                        |  User-facing fast             
  +-------------------------------------------+  KV store functionality       
        |          |      |         |                                         
        v          |      v         v                                         
  +---------+      |  +--------+ +------------+                               
  | Control |      |  | Log(n) | | Log(n+1)   |                               
  +---------+      |  +--------+ +------------+                               
                   |                                                          
                   v                                                          
  +-------------------------------------------+                               
  | Nv_map_ss (layer 3)                       |  KV store                     
  +-------------------------------------------+  (optimized for batch update) 
       |                           |                                          
       |                           v                                          
       |                        +--------+                                    
       |                        | Values |                                    
       v                        +--------+                                    
  +-------------------------------------------+                               
  | Nv_map_ii (layer 2)                       |  Non-volatile int->int map    
  +-------------------------------------------+                               
      |                |                |                                     
      v                |                v                                     
  +-----------+        |           +----------+                               
  | Partition |        |           | Freelist |                               
  +-----------+        |           +----------+                               
                       v                                                      
  +-------------------------------------------+                               
  | Bucket store (layer 1)                    |  A store of buckets           
  +-------------------------------------------+  ie (int,int) bindings        
       |            |           |                                             
       v            v           v                                             
  +-----------+ +-----------+ +-----------+                                   
  | Bucket    | | Bucket    | | Bucket    |                                   
  +-----------+ +-----------+ +-----------+                                   

```

(NOTE: diagram using textik, stored on gdrive as kv-hash-arch)

For easy recall, we have the following table:

| Layer | Layer name   | Files                                      |
| ----- | ------------ | ------------------------------------------ |
| 1     | Bucket store | buckets.data                               |
| 2     | Nv_map_ii    | part_nnnn, freelist_nnnn                   |
| 3     | Nv_map_ss    | values.data                                |
| 4     | Frontend     | ctl.data, log_nnnn (and previous log_nnnn) |



## Converting string keys and values to integers

It is much more efficient to work with ints rather than arbitrary strings.

For a given string key, we use a hash function to convert to a corresponding int key. For buckets and partitions, it is this int key that we work with. [See the addendum below for how to deal with hash collisions.]

For a given string value, we record the value by writing it to the end of a global "values file". The offset in the file is then the "int value" for the corresponding string value.

We think in terms of arbitrary keys and values, but it is important to remember that for the bucket and partition these are actually ints (hashes for keys and offsets-in-values-file for values).



## Buckets and the bucket store

A bucket contains a small number of (int)key-(int)value pairs. Typically the keys are all close together, and part of some range $l_i \le k < h_i$. In our implementation, bucket keys are ints (hashes of an actual string key) and values are also ints (offsets in the "values file").

A bucket resides in a single block on disk. For a block size of 4096 bytes, we can store at most 255 keys with their associated values (512 64-bit-8-byte ints; 2 are needed for metadata; 510 ints available for kvs).

We write buckets to disk at block-aligned offsets. Thus, bucket update is (we hope) atomic. The order that buckets flush to disk does not affect correctness.

Adding a new kv to a bucket is fast. Searching is fast. A bucket is actually a block-sized int array, so going to/from disk is fast.

Sometimes, a bucket will become full. In this case, the bucket is split into two. Let's say the original range is $l_0 \le ... < h_0$. Let's say the new buckets are $b_1, b_2$. Then $l_0 = l_1, l_1 < h_1, h_1 = l_2, l_2 < h_2, h_2 = h_0$, i.e., each bucket corresponds to some subrange of the original range. 

```
l0        h0   <- original range
l1 < h1        <- b1 subrange
     l2 < h2   <- b2 subrange
     
     ^ The point h1 (equal to l2) is the point where we divide the original range.
```



When this happens, we also have to update the partition (see below), with the information that $l_1$ maps to (the identifier of) $b_1$ and $l_2$ maps to  $b_2$ (previously, the partition had a mapping for $l_1$ only).

A bucket store is just a collection of buckets, typically backed by a file or mmap. A freelist of free buckets that can be used at the next merge is also maintained.



## Partition (map from key to bucket)

To lookup the value for a given (int)key, we need to identify the corresponding bucket. This is the purpose of the partition. 

Given the set of buckets $b_i$, a partition maps $l_i$ (the lower bound for the bucket $b_i$) to a bucket identifer (actually, the block location of the bucket on disk). A partition is implemented as a simple binary search tree, from int to int.

To find the bucket for a key $k$, we locate the $l_i$ which is less than or equal to $k$, and which is the greatest such. This can be done efficiently since the partition is a binary search tree which supports this operation. [NOTE we use Jane St. Base Map implementation, since OCaml's Stdlib.Map doesn't provide this operation]. Thus, $l_i \le k < h_i$, and the partition maps $l_i$ to the bucket identifier for the bucket $b_i$ we want.



## nv-map-ii: a non-volatile map (int -> int)

Now we have the components to build a non-volatile map from int to int. It consists of a partition and a collection of buckets (say, 10k buckets initially). Buckets are already stored on disk. When the partition changes (because of bucket splits), we can write that to disk as well. In fact, we typically perform batch operations on this non-volatile map, so we only write the partition to disk after a batch operation.





## nv-map-ss: a non-volatile map (string -> string)

The only thing left to do is to add a wrapper round the nv-map-ii so that keys and values can be strings.

For the "insert k v" operation, we write the value to the values file, and get an offset (int) in return. We hash the key to a hash (int). We then store the (hash,offset) in the nv-map-ii.

For the "find k" operation, we hash the key to a hash (int). We use the nv-map-ii to lookup the relevant (int) value. If there is no corresponding value, then the key k is not in the map. Otherwise, the (int) value is an offset in the values file and we can read the corresponding (string) value from this file, and return it.





## Frontend log rotation

We prefer to execute operations against the nv-map-ss as a batch. This allows us to take advantage of any locality (perhaps we need to make multiple insertions into a bucket during a batch). We also have a requirement that operations like insert are "as fast as possible" and that concurrent readers communicate only via disk. 

For these reasons, we add a log in front of the nv-map-ss. The log records all new inserts as soon as they are made, so that readers can notice these updates in a timely fashion. Periodically the log is merged as a batch with the main store, and a new log is started. 

The merge is executed concurrently by another process, whilst the main process continues to process operations such as insert and find. Until the merge completes, we have to keep the old log entries in memory when processing requests. After the merge completes, all the updates corresponding to the old log entries have been made on disk. If, during the merge, the partition has changed, the merge process writes the updated partition to disk, where it is read by the main process at the start of the next merge.

It is worth noting that the merge could be carried out by multiple processes, each working with a different subset of the buckets, and each merging partition updates back into the main partition. At the moment this is not necessary because each merge completes quickly relative to how long it takes for the log to fill (i.e., the main thread never needs to wait any time for a merge to complete before starting a new merge).





## Control file

In order to keep all processes coordinated, we maintain a control file, which consists of several integers representing the states of various fields. The control file easily fits within a block, and so there is no problem with updating it atomically etc.

The fields in the control file are as follows:

| Field              | Description                                                  |
| ------------------ | ------------------------------------------------------------ |
| current_log        | A mono-increasing "generation number", used to identify the current log file. For example, if current_log is 1234, then the latest log is log_1234. |
| current_partition  | Used to identify the current partition. For example, if current_partition is 1234, then the latest partition is in file part_1234. Typically a new partition is written with whatever the current_log value is. However, since the partition is not necessarily updated on each merge, the current_partition can lag the current_log. FIXME why not just use the rename trick to update the partition? |
| log_last_flush_pos | The position in the current_log when the last flush call to the log completed. Data upto this point is guaranteed to be on-disk. Data after this point may be lost on crash. |
| last_merge         | The number of the last log that was successfully merged. This is used during recovery to decide whether or not to replay the merge of an old log. |

* In addition, the lookaside table is stored under filename "lookaside_nnnn", where nnnn is the current generation. FIXME TODO



## Summary of files involved

In the standard configuration, we have the following files:

| File          | Description                                                  |
| ------------- | ------------------------------------------------------------ |
| buckets.data  | The potentially-huge store of buckets.                       |
| values.data   | The values file (string values are converted to integer offsets in this file); also potentially huge. |
| ctl.data      | The control file. Small.                                     |
| part_nnnn     | A partition file.                                            |
| log_nnnn      | A log file.                                                  |
| freelist_nnnn | A freelist.                                                  |



## Recovery after a crash

The code should provide crash consistency. Various technologies are employed:

* Rename-trick: The "rename trick" - writing data to a `file.tmp` and then renaming to `file`; for this to work correctly, we need to flush `file.tmp`; flush the containing directory; perform the rename; then flush the containing directory again.
* Crash-safe-log: The module `Log_file` contains an implementation of a crash-safe append-only file. (`Log_file_opt` contains an alternative "optimistic" log implementation.)
* Block-writes: Block-sized, block-aligned writes are assumed to commit to disk atomically (but not necessarily in order).
* Record-flush: flushed offsets for (append-only) files are recorded in the control file. Downside: to flush an individual file, we have to then also flush the control file afterwards, which is potentially expensive.
* DB: We can use a DB like LMDB or SQLite. Compared to raw files, these are generally much slower, and there is less control eg over flushing (DB changes commit as a transaction, which includes the trailing flush). The advantage is that LMDB et al. are well-tested pieces of code.

For the various files involved, we describe the technologies used to ensure crash consistency:

| File           | Crash-safe technique                                         |
| -------------- | ------------------------------------------------------------ |
| buckets.data   | Block-writes                                                 |
| partition.data | Rename-trick                                                 |
| freelist.data  | Rename-trick                                                 |
| values.data    | Record-flush (FIXME todo)                                    |
| log_nnnn       | Record-flush (FIXME todo)                                    |
| ctl.data       | Initialization using rename-trick (FIXME todo); block-writes (only 1 block) |

The sequence of events when recovering looks as follows:

1. All `.tmp` temporary files are deleted (they were created before the crash when attempting to use the rename trick)
2. The bucket store can be opened directly.
3. The partition can be read from the last saved version.
4. The freelist can be read from the last saved version.
5. The values file can be truncated to "last flushed position" then opened.
6. The current log_nnnn file can be truncated to "last flushed position" then opened. The previous log should be safely on disk, and what is needed is to replay the merge using this old log, if necessary.

Once ctl.data exists, it is guaranteed that:

1. buckets.data exists
2. partition.data exists
3. freelist.data exists FIXME check this
4. values.data exists
5. log_nnnn and the previous log (if any) exist

Thus, once ctl.data exists, then all the files we need to recover after a crash will exist and will correctly represent a consistent state just before the crash.

The system assumes it has complete control over the directory where it stores its files. It may be that some file is accidentally deleted by the user, outside the control of the system. The system should detect this and issue a warning, then fail to start. If a user modifies one of the internal files (say, buckets.data, or values.data, or one of the log files) then all bets are off, since the system relies on the data held in these files being the data that was previously written.



# Addenda

The following sections discuss particular topics.

## Requirements, and design justification

We need to implement a key-value store, where the set of keys is huge (much larger than main memory) and the active set of keys is huge (so, we expect that we potentially need to go to disk for each operation). 

An additional requirement is that the implementation support a single writer with multiple concurrent readers, and the readers must not block. The readers and the writer must communicate only by disk (no RPC). Further, inserts and finds must be "as fast as possible".

A B-tree is the obvious solution. However, B-tree code can be somewhat complex. Further, it is common to cache the internal B-tree nodes in main memory, in order to ensure good performance. The design of kv-hash proceeds from these observations. It assumes that the non-leaf nodes can be kept easily in main memory, and easily persisted to disk, and replaces them with the partition datastructure. Thus, only leaves are kept on disk, in the form of buckets. We still have leaf-splitting, as occurs with a B-tree.





## Key hash collisions

We use xxhash which has a reasonable reputation for being uniformly distributed and having other good properties. After adding a billion keys with unique hashes, we have a billion hashes in the store. However, since we are using 63-bit ints, *for each used hash*, there are *more than 1 billion free hashes*. So, the chance of collision is low. But still we might worry about it.

Suppose we have two keys with the same hash. We execute $\textit{insert}(k_1,v_1)$ and $\textit{insert}(k_2,v_2)$. Without further precaution, the (int) $v_2$ would overwrite the $v_1$ in the relevant bucket, leading to incorrect behaviour. The way to avoid this is to use the values file to store not just a value, but a (key,value) pair.

In this case, the (int) $v_1$ is an offset into the values file, which records the (string,string) $(k_1,v_1)$ as a pair. Then when we execute $\textit{insert}(k_2,v_2)$ we check the value that we find already in the bucket and discover that this old value (int) $v_1$ corresponds to a (string) $k_1$ in the values file, rather than the (string) $k_2$ we were expecting. 

At this point we have identified that there is a hash collision. To deal with these (extremely few) hash collisions, we have a separate lookaside table (which is also stored and loaded from disk) which contains the (string) keys and values for clashing keys. In this example, the lookaside table would contain both $(k_1,v_1)$ and $(k_2,v_2)$.

The case, where the second operation is $\textit{find}(k_2)$, is similar. Having found the (int) $v_1$ in the bucket, we proceed to convert back to a string value by checking the values file. At this point we discover that the value was associated to another key $k_1$. So, we return "none". If we want, we can promote $k_1$ and/or $k_2$ to the lookaside table at this point, or alternatively wait for an insert on $k_2$.

NOTE the "values file" should then more properly be called the "key-values file".



## How big does the partition get?

The partition ensures that we can lookup an (int) key with at most 1 disk read. Similarly we need at most 1 disk read and 1 disk write to update an (int) key with an (int) value.

The partition is implemented as a binary search tree, from int to int. For 1M partition-kvs (corresponding to 1M buckets, or roughly 200M real kvs stored in the buckets), each leaf perhaps takes up 3 ints worth, ie 24 bytes. So, the entire partition consumes of the order of 24MB for the leaves, and perhaps the same again for the internal nodes, 48MB in total. This amount scales linearly. 

| Partition-kvs | Real kvs (200 * partition-kvs) | Partition size (in mem) |
| ------------- | ------------------------------ | ----------------------- |
| 1M            | 200M                           | 48MB                    |
| 10M           | 2B                             | 480MB                   |
| 100M          | 20B                            | 4.8GB                   |

For 2B real kvs, 480MB is significant to reload when the partition changes. For 20B real kvs, 4.8GB is a significant amount of main memory to have to maintain.

Around 10M partition-kvs loading a partition becomes a slightly lengthy process. We can ameliorate the partition reload by writing partition updates rather than the full partition. 

For 100M partition-kvs (20B real kvs) we need to have 4.8GB of memory to hold the partition. A desktop machine might have 16GB of memory, so this is not a problem. For 400M partition-kvs (80B real kvs), desktop machines will struggle. For 1B partition-kvs (200B real kvs), we need about 48GB to store the partition. So this is really the domain of server machines.

Anyway, at this point we should probably replace the partition by a bona-fide B-tree. However, unless we keep the B-tree nodes in memory (and consuming 48GB), performance will suffer since we need multiple disk reads (for example) to locate an (int) key. However, assuming we keep all but the leaf nodes and their parents in main memory, we should be able to handle huge numbers of kvs with a B-tree, with at most 2 reads from storage to locate an (int) key. 

It it worth noting that the partition only changes during a merge. So the main process, and the RO processes, can simply mmap a sorted array for the partition. Of course, this likely needs to be kept fully in main memory, but at least only one instance of the memory is required for all processes (the mmap memory is shared between processes). So only the merge process needs to keep the partition in main memory.

An alternative to holding the partition wholly in memory is to store only updates in memory, and store the main partition as an mmap'ed array of integers.



## Typical file sizes and memory usage, Tezos use case (570M kvs)

One use case for kv-hash is as a backend index for Irmin (which is used as the store for Tezos). A typical replay of Tezos commits, starting from the genesis block, and involving over 1.3M commits, gives the following file sizes:

| File         | Component                      | Size on disk            | Can be shrunk?                  |
| ------------ | ------------------------------ | ----------------------- | ------------------------------- |
| store.pack   | Main irmin store (not kv-hash) | 49GB                    | NA                              |
| buckets.data | kv-hash                        | 24GB                    | Y, to 12GB (bucket reclaim)     |
| partition    | kv-hash                        | 103MB (3070825 entries) | Y, to 48MB (marshal format)     |
| values.data  | kv-hash                        | 16GB                    | Y, to 12GB (better marshalling) |

Comments:

* How many kvs are stored in the buckets? 3M buckets, each of which stores on average 75% * 255 keys, approx 190 keys, giving roughly 570M keys (and the same number of values)
* buckets.data currently does no GC to reclaim buckets after splitting; given the active buckets are 3070825, we estimate around 12GB of buckets.data is live, the rest can be reclaimed; in addition, note that buckets are, on average, only 75% full (a consequence of the design chosen)
* The partition is kept in memory and synced to disk after a merge. The current format is not efficient, and could be improved (3M entries, each of two ints, gives 48MB total). Even so, keeping the partition in memory is likely to consume upwards of 100MB, which is significant. Actually, only the merge process has to keep this in memory  - since it is only during the merge that partition changes occur. The main process could mmap a sorted array representing the partition, as could the RO processes. Indeed, it is likely that the merge process, rather than keep the whole partition in memory, could use the mmap'ed partition, together with a list of updates, to reduce the memory usage to effectively small and constant space.
* values.data is storing 500M values, which are (say) 3 ints (24 bytes) in the Tezos use case; efficient marshalling would reduce the values.data size to 12GB say; values.data currently does not store the keys; it should do (and we should monitor for hash collisions); this would increase the values.data file significantly (perhaps, to 24GB or more)
* Taken together, we have that the file sizes for the kv-hash components are roughly the same size as the main store.pack, which seems too much. For Tezos, the keys and values are fixed size, so some space could be reclaimed by taking advantage of this fact (eg for values.data file - we know each value is fixed length). 



## Bucket reuse

In order to keep the size of the main buckets.data store low, it is necessary to recycle old buckets after splitting. This introduces some possible correctness issues, since an out-of-date partition may allow access to a recycled bucket via its old binding in the partition. In fact, everything works out providing some precautions are taken.

Let's look at a typical sequence of events:

```
--log(n)---|--log(n+1)------>
           |--merge(n)---|
                         |--part(n)--->
```

Here, time increases to the right. log(n) is the period log(n) is written to. When the log is filled, a merge process (for that log) is initiated, and log(n+1) is written to. When the merge completes, the partition is written to disk and becomes the new partition for accessing the main store. 

Several questions arise, such as: What happens if we use an old partition for accessing the main store, when there is a new partition available? However, we focus on the question of what happens to buckets that get recycled. Consider the sequence of events over 2 log rotations:



```
log      |n---|n+1--------|n+2----
merge    |    |n---|      |n+1---|
partition|         |n------------|n+1-----
b p(n-1) |----------------|         // validity of b for p(n-1)
error    |       ^-----------^      // error case
```

Suppose bucket b is split during merge n. This bucket is not reused straightaway (this would certainly lead to correctness issues). However, it is available for reuse during merge (n+1).

Note that partition n -- p(n) for short -- becomes accessible after merge n, and that bucket b is not accessible via p(n). If b is reused during merge(n+1), then it becomes accessible via the next partition p(n+1).

What would it take to incorrectly access b? We would need a partition that referenced b (so, p(n-1), the partition used before merge n), and we would need to access b at a point that it's data became invalid (for that partition), that is, from the beginning of merge(n+1) onwards. With respect to p(n-1), b is valid upto the beginning of merge(n+1). This is the "b p(n-1)" line in the diagram.

Suppose we have a concurrent process servicing requests. Let's assume that the process always checks for a new partition before servicing a request. In order for something to go wrong, a request would have to start before p(n) became available (so that it used p(n-1) to access the bucket data), and finish after the start of merge(n+1) (so that the bucket b holds "incorrect data" for p(n-1)). This is the "error" line above. The first observation to make is that, given the timings we observe, this would be a very long time to service a single request. One is tempted to say that this "can never happen" in normal operation. This is perhaps true, given various assumptions about scheduling of processes, length of time of various operations, max time to complete a request etc. However, it is better to have a correctness criterion that holds without all these assumptions. For example, what if the log length is 1, rather than 1M? What if the time between the start of merge n and the end of merge n+1 is shorter than the average time to serve a request? We want to feel confident that we understand these cases. 

*In order to rule out the error case, it suffices to ensure that we check the current partition at the start of servicing a request, and check it again at the end.* If it hasn't changed we can be sure we accessed the main bucket store correctly. If it has changed, we can simply load the new partition and retry the request, and check the partition again at the end. Obviously this criterion is overkill, and one could be much more nuanced (and so avoid a few unnecessary retries). However, since in our setup retries are expected to be extremely rare anyway, this is enough.

We now return to an earlier question: What happens if we use an old partition for accessing the main store, when there is a new partition available? In fact, the old partition p(n-1) is valid (but not up-to-date) right up to the moment that merge(n+1) starts (and bucket b can be reused). Thus, validity of p(n-1) is the same as "validity of b for p(n-1)", since the only way the partition can become invalid is if b is reused. The situation is further complicated because concurrent processes are supposed to use the logs to service requests as far as possible, only resorting to the main bucket store if necessary. If log(n) is kept around after merge(n) completes, then it doesn't even matter that p(n-1) is (valid but) out of date wrt. the main buckets store, since log(n) provides the recent updates. In the current implementation, log(n) is kept till the end of log(n+1) (as opposed to being deleted after merge(n)). p(n-1) ++ log(n) ++ log(n+1) is then valid and up to date just up to the start of merge(n+1). This makes clear that it is not so necessary to update the partition as soon as a new one becomes available, and that the criterion above is perhaps overly strong.

FIXME this section could do with some reworking

Timelines:

```
----> time

0        1       2       3       4       5       6       7       8       9       10
+--------+-------+-------+-------+-------+-------+-------+-------+-------+-------+
         log(1)          log(2)          log(3)          log(4)
                         m(1)----X       m(2)----X       m(3)----X
part(0)----------------------------------X
                                 part(1)-----------------X
                                                 part(2)-----------------X
                                                                 part(3)-----------------X
```
Explanation:
0. The initial partition is created
1. We start writing to log(1)
2. -
3. We finish log(1), initiate a merge of log(1) -- m(1) -- and proceed with log(2)
4. m(1) finishes, part(1) is written and becomes a valid partition; part(0) is also valid since no buckets are recycled until m(2) starts
5. Finish log(2), initiate m(2), start log(3). NOTE At this point, part(0) is no longer valid because blocks that became garbage in m(1) may be reused in m(2).
6. m(2) finishes, part(2) is written
7. Finish log(3), initiate m(3), start log(4). NOTE part(1) no longer valid.
8. m(3) finishes, part(3) is written


