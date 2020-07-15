Tests for Clevel Hashing
=======================

## Experimental Setup

#### PM environment

PM (Intel Optane DC Persistent Memory) is required to be configured in **App Direct** mode. Please refer to the [manual](https://software.intel.com/content/www/us/en/develop/articles/quick-start-guide-configure-intel-optane-dc-persistent-memory-on-linux.html) for the background and configurations for PM.

#### DRAM environment

Run the following command before running tests (for debug only).
```sh
$ export PMEM_IS_PMEM_FORCE=1
```

#### Workload format
The workloads are generated using [YCSB](https://github.com/brianfrankcooper/YCSB). We simplified the trace formats for ease of tests. Each line in a workload is a query. The format for each line is "`OP` `KEY`". Possible values for `OP` include "READ", "INSERT", "UPDATE", and "DELETE".

## Tests
- `clevel_hash_cli`: a simple test for queries
```
USAGE:  ./clevel_hash_cli <pool_path> <cmd> <key>

    pool_path: the pool file required for PMDK
    cmd: a query for a key, including "print" (search), "alloc" (insert), and "free" (delete)
    key: a key (integer) required for the query
```

- `clevel_hash_resize`: a resizing test for continuous insertions. Print the load factor per 10k insertions.
```
USAGE:  ./clevel_hash_resize <pool_path> <load_file>

    pool_path: the pool file required for PMDK
    load_file: an insert-only workload file
```

- `clevel_hash_ycsb`: a test for medium workloads. The number of queries in a workload is 16 millions by default, which can be configured by modifying the MACRO `READ_WRITE_NUM`.
```
USAGE:  ./clevel_hash_ycsb <pool_path> <load_file> <run_file> <thread_num>

    pool_path: the pool file required for PMDK
    load_file: a workload file for the load phase
    run_file: a workload file for the run phase
    thread_num: the number of threads (>=2, including the background threads for rehashing).
```

- `clevel_hash_ycsb_macro`: a test for large workloads. The number of queries in a workload is 64 millions by default, which can be configured by modifying the MACRO `READ_WRITE_NUM`.
```
USAGE:  ./clevel_hash_ycsb_macro <pool_path> <load_file> <run_file> <thread_num>

    pool_path: the pool file required for PMDK
    load_file: a workload file for the load phase
    run_file: a workload file for the run phase
    thread_num: the number of threads (>=2, including the background threads for rehashing).
```