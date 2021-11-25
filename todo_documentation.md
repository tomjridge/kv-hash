NOTE on block recycling: Other processes (eg read only processes)
   should always start a lookup by checking whether they have the
   current partition. In addition they must always check for partition
   change just before returning any result; if the partition has
   changed since the process started the operation, the process must
   retry the operation; this ensures that recycled blocks are not
   misinterpreted leading to incorrect results.

   Alternatively, if we can ensure that any other process can execute
   a single operation in less time than it takes from the start of one
   merge to the start of another, then we are (probably) safe, since
   no block will be recycled during that time (we recycle old blocks
   on the NEXT merge). 
