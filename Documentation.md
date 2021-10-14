# Kv-hash documentation

This supplements the documentation in the code itself.

Kv-hash is a library to implement a key-value store. Keys and values can be arbitrary strings. 

For the individual components -- buckets and partitions -- we work with the int hash of a given key (so, an int key corresponding to some actual string key) and an int value (the offset of the string value in some "values file").

The main idea is to maintain a "partition" in memory that describes how to map an (int) key to a particular on-disk bucket. There are many buckets, each corresponding to a distinct part of the key space, i.e., for bucket $i$, and int key $k$ in that bucket, we have  $l_i \le k < h_i$ for some $l_i, h_i$, and the ranges are disjoint and span the entire int key space ($0... \textit{max_int}$). We can then add a new kv by adding to the corresponding bucket, or find the value corresponding to a key by looking in the corresponding bucket.

There are several concepts we need to describe:

* converting keys to ints; converting values to ints; the "values file"
* bucket 
* partition
* non-volatile map (int -> int)
* non-volatile map (string -> string)
* frontend log rotation
* control file



## Converting string keys and values to integers

It is much more efficient to work with ints rather than arbitrary strings.

For a given string key, we use a hash function to convert to a corresponding int key. For buckets and partitions, it is this int key that we work with. [See the addendum below for how to deal with hash collisions.]

For a given string value, we record the value by writing it to the end of a global "values file". The offset in the file is then the "int value" for the corresponding string value.

We think in terms of arbitrary keys and values, but it is important to remember that for the bucket and partition these are actually ints (hashes for keys and offsets-in-values-file for values).



## Bucket

A bucket contains a small number of (int)key-(int)value pairs. Typically the keys are all close together, and part of some range $l_i \le k < h_i$. In our implementation, bucket keys are ints (hashes of an actual string key) and values are also ints (offsets in the "values file").

A bucket resides in a single block on disk. For a block size of 4096 bytes, we can store at most 255 keys with their associated values.

We write buckets to disk at block-aligned offsets. Thus, bucket update is (we hope) atomic. The order that buckets flush to disk does not affect correctness.

Adding a new kv to a bucket is fast. Searching is fast. A bucket is actually an int array, so going to/from disk is fast.

Sometimes, a bucket will become full. In this case, the bucket is split into two. Let's say the original range is $l_0 \le ... < h_0$. Let's say the new buckets are $b_1, b_2$. Then $l_0 = l_1, l_1 < h_1, h_1 = l_2, l_2 < h_2, h_2 = h_0$, i.e., each bucket corresponds to some subrange of the original range. 

```
l0        h0   <- original range
l1 < h1        <- b1 subrange
     l2 < h2   <- b2 subrange
     
     ^ The point h1 (equal to l2) is the point where we divide the original range.
```



When this happens, we also have to update the partition, with the information that $l_1$ maps to (the identifier of) $b_1$ and $l_2$ maps to  $b_2$ (previously, the partition had a mapping for $l_1$ only).



## Partition

For a given (int)key, we need to identify the corresponding bucket. This is the purpose of the partition. 

Given the set of buckets $b_i$, a partition maps $l_i$ (the lower bound for the bucket $b_i$) to a bucket identifer (actually, the block location of the bucket on disk). A partition is implemented as a simple binary search tree, from int to int.

To find the bucket for a key $k$, we locate the $l_i$ which is less than or equal to $k$, and which is the greatest such. This can be done efficiently since the partition is a binary search tree which supports this operation (NOTE we use Jane St. Base Map implementation, since OCaml's Stdlib.Map doesn't provide this operation). Thus, $l_i \le k < h_i$, and the partition maps $l_i$ to the bucket identifier for the bucket $b_i$ we want.



## Non-volatile map (int -> int), aka nv-map-ii

Now we have the components to build a non-volatile map from int to int. It consists of a partition and a collection of buckets (say, 10k buckets initially). Buckets are already stored on disk. When the partition changes (because of bucket splits), we can write that to disk as well. In fact, we typically perform batch operations on this non-volatile map, so we only write the partition to disk after a batch operation.





## Non-volatile map (string -> string), aka nv-map-ss

The only thing left to do is to add a wrapper round the nv-map-ii so that keys and values can be strings.

For the "insert k v" operation, we write the value to the values file, and get an offset (int) in return. We hash the key to a hash (int). We then store the (hash,offset) in the nv-map-ii.

For the "find k" operation, we hash the key to a hash (int). We use the nv-map-ii to lookup the relevant (int) value. If there is no corresponding value, then the key k is not in the map. Otherwise, the (int) value is an offset in the values file and we can read the corresponding (string) value from this file, and return it.





## Frontend log rotation

We prefer to execute operations against the nv-map-ss as a batch. This allows us to take advantage of any locality (perhaps we need to make multiple insertions into a bucket during a batch). We also have a requirement that operations like insert are "as fast as possible" and that concurrent readers communicate only via disk.

For these reasons, we add a log in front of the nv-map-ss. The log records all new inserts as soon as they are made, so that readers can notice these updates in a timely fashion. Periodically the log is merged as a batch with the main store, and a new log is started. 

The merge is executed concurrently by another process, whilst the main process continues to process operations such as insert and find. Until the merge completes, we have to keep the old log entries in memory when processing requests. After the merge completes, all the updates corresponding to the old log entries have been made on disk. If, during the merge, the partition has changed, the merge process writes the updated partition to disk, where it is read by the main process at the start of the next merge.

It is worth noting that the merge could be carried out by multiple processes, each working with a different subset of the buckets, and each merging partition updates back into the main partition. At the moment this is not necessary because each merge completes quickly relative to how long it takes for the log to fill (i.e., the main thread never needs to wait any time for a merge to complete before starting a new merge).





## Control file

In order to keep all processes coordinated, we maintain a control file, which consists of several integers.

* The current "generation"; the generation is changed every time we move to a new log file; the log file is named "log_nnnn", where nnnn is the generation number.
* The most recent partition number; the partition is stored in a file "part_mmmm" where mmmm is the most recent partition number. FIXME perhaps we just store this in "part_nnnn" and rename from previous when not changed?

In addition, the lookaside table is stored under filename "lookaside_nnnn", where nnnn is the current generation. FIXME TODO





## Addendum: key hash collisions

We use xxhash which has a reasonable reputation for being uniformly distributed and having other good properties. After adding a billion keys with unique hashes, we have a billion hashes in the store. However, since we are using 63-bit ints, *for each used hash*, there are *more than 1 billion free hashes*. So, the chance of collision is low. But still we might worry about it.

Suppose we have two keys with the same hash. We execute $\textit{insert}(k_1,v_1)$ and $\textit{insert}(k_2,v_2)$. Without further precaution, the (int) $v_2$ would overwrite the $v_1$ in the relevant bucket, leading to incorrect behaviour. The way to avoid this is to use the values file to store not just a value, but a (key,value) pair.

In this case, the (int) $v_1$ is an offset into the values file, which records the (string,string) $(k_1,v_1)$ as a pair. Then when we execute $\textit{insert}(k_2,v_2)$ we check the value that we find already in the bucket and discover that this old value (int) $v_1$ corresponds to a (string) $k_1$ in the values file, rather than the (string) $k_2$ we were expecting. 

At this point we have identified that there is a hash collision. To deal with these (extremely few) hash collisions, we have a separate lookaside table (which is also stored and loaded from disk) which contains the (string) keys and values for clashing keys. In this example, the lookaside table would contain both $(k_1,v_1)$ and $(k_2,v_2)$.

The case, where the second operation is $\textit{find}(k_2)$, is similar. Having found the (int) $v_1$ in the bucket, we proceed to convert back to a string value by checking the values file. At this point we discover that the value was associated to another key $k_1$. So, we return "none". If we want, we can promote $k_1$ and/or $k_2$ to the lookaside table at this point, or alternatively wait for an insert on $k_2$.

NOTE the "values file" should then more properly be called the "key-values file".





## Addendum: requirements, and design justification

We need to implement a key-value store, where set of keys is huge (much larger than main memory) and the active set of keys is huge (so, we expect that we potentially need to go to disk for each operation). 

An additional requirement is that the implementation support a single writer with multiple concurrent readers, and the readers must not block. The readers and the writer must communicate only by disk (no RPC). Further, inserts and finds must be "as fast as possible".

A B-tree is the obvious solution. However, B-tree code can be somewhat complex. Further, it is common to cache the internal B-tree nodes in main memory, in order to ensure good performance. The design of kv-hash proceeds from these observations. It assumes that the non-leaf nodes can be kept easily in main memory, and easily persisted to disk, and replaces them with the partition datastructure. Thus, only leaves are kept on disk, in the form of buckets. We still have leaf-splitting, as occurs with a B-tree.







## Addendum: how big does the partition get?

The partition ensures that we can lookup an (int) key with at most 1 disk read. Similarly we need at most 1 disk read and 1 disk write to update an (int) key with an (int) value.

The partition is implemented as a binary search tree, from int to int. For 1M kvs, each leaf perhaps takes up 3 ints worth, ie 24 bytes. So, the entire partition consumes of the order of 24MB for the leaves, and perhaps the same again for the internal nodes, 48MB in total. This amount scales linearly. For 10M it is 480MB. For 100M it is 4.8GB. For 10M kvs, 480MB is significant to reload when the partition changes. For 100M kvs, 4.8GB is a significant amount of main memory to have to maintain.

Around 10M kvs loading a partition becomes a slightly lengthy process. We can ameliorate the partition reload by writing partition updates rather than the full partition. 

For 100M kvs we need to have gigabytes of memory to hold the partition. A desktop machine might have 16GB of memory, so 100M kvs is not problematic. For 400M kvs, desktop machines will struggle. For 1B kvs, we need about 48GB to store the partition. So this is really the domain of server machines.

Anyway, at this point we should probably replace the partition by a bona-fide B-tree. However, unless we keep the B-tree nodes in memory (and consuming 48GB), performance will suffer since we need multiple disk reads (for example) to locate an (int) key. However, assuming we keep all but the leaf nodes and their parents in main memory, we should be able to handle huge numbers of kvs with a B-tree, with at most 2 reads from storage to locate an (int) key. 