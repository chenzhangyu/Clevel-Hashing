Clevel Hashing
===============
## Introduction
Clevel hashing is a lock-free PM hashing index with asynchronous resizing, which supports lock-free operations for search/insertion/update/deletion. This repository is the implementation of "Lock-free Concurrent Level Hashing for Persistent Memory", in Proceedings of the USENIX Annual Technical Conference (USENIX ATC), 2020.

## Implementation & Tests
We implement clevel hashing with PMDK and provide query interfaces similar to the `concurrent_hash_map` in libpmemobj-cpp. The [implementation](include/libpmemobj%2B%2B/experimental/clevel_hash.hpp) and [tests](tests/clevel_hash) for clevel hashing are based on the libpmemobj-cpp ([commit@26c86b4699](https://github.com/pmem/libpmemobj-cpp/tree/26c86b46997d25c818b246f2a143d2248503cc67)). 

## Compilation

#### Requirements:
- cmake >= 3.3
- libpmemobj-dev(el) >= 1.4 (http://pmem.io/pmdk/)
- compiler with C++11 support

#### How to build
We only ran our tests on Linux (Ubuntu 18.04). Run the following commands for compilation:
```sh
$ mkdir build
$ cd build
$ cmake ..
$ make -j
```

To compile with TBB enabled, please run the following commands:
```sh
$ mkdir build
$ cd build
$ cmake .. -DUSE_TBB=1
$ make -j
```

## Run
Please refer to the [instructions](tests/clevel_hash) for the use of test files.

## Limitation
- Current tests are used for the performance evaluation and not designed for `ctest`.

## Contact
If you have any problems, please feel free to contact me.
- Zhangyu Chen (chenzy@hust.edu.cn)

**\*\*\*\*Following documentation comes from original libpmemobj-cpp\*\*\*\***

------


libpmemobj-cpp
===============

[![Build Status](https://travis-ci.org/pmem/libpmemobj-cpp.svg?branch=master)](https://travis-ci.org/pmem/libpmemobj-cpp)
[![Build status](https://ci.appveyor.com/api/projects/status/github/pmem/libpmemobj-cpp?branch/master?svg=true&pr=false)](https://ci.appveyor.com/project/pmem/libpmemobj-cpp/branch/master)
[![Coverity Scan Build Status](https://scan.coverity.com/projects/15911/badge.svg)](https://scan.coverity.com/projects/pmem-libpmemobj-cpp)
[![Coverage Status](https://codecov.io/github/pmem/libpmemobj-cpp/coverage.svg?branch=master)](https://codecov.io/gh/pmem/libpmemobj-cpp/branch/master)

C++ bindings for libpmemobj (https://github.com/pmem/pmdk)
More information in include/libpmemobj++/README.md

# How to build #

## Requirements: ##
- cmake >= 3.3
- libpmemobj-dev(el) >= 1.4 (http://pmem.io/pmdk/)
- compiler with C++11 support

## On Linux ##

```sh
$ mkdir build
$ cd build
$ cmake ..
$ make
$ make install
```

#### When developing: ####
```sh
$ ...
$ cmake .. -DCMAKE_BUILD_TYPE=Debug -DDEVELOPER_MODE=1
$ ...
$ ctest --output-on-failure
```

#### To build packages ####
```sh
...
cmake .. -DCPACK_GENERATOR="$GEN" -DCMAKE_INSTALL_PREFIX=/usr
make package
```

$GEN is type of package generator and can be RPM or DEB

CMAKE_INSTALL_PREFIX must be set to a destination were packages will be installed

#### To use Intel(R) Threading Building Blocks library ####

By default concurrent_hash_map uses pmem::obj::shared_mutex internally. But read-write mutex from Intel(R) Threading Building Blocks library can be used instead to achieve better performance. To enable it in your application set the following compilation flag:
- -DLIBPMEMOBJ_CPP_USE_TBB_RW_MUTEX=1

If you want to build tests for concurrent_hash_map with read-write mutex from Intel(R) Threading Building Blocks library, run cmake with ```-DUSE_TBB=1 -DTBB_DIR=<Path to Intel TBB>/cmake``` option.

Intel(R) Threading Building Blocks library can be downloaded from the official [release page](https://github.com/01org/tbb/releases).

#### To use with Valgrind ####

In order to build your application with libpmemobj-cpp and
[pmemcheck](https://github.com/pmem/valgrind) / memcheck / helgrind / drd,
Valgrind instrumentation must be enabled during compilation by adding flags:
- LIBPMEMOBJ_CPP_VG_PMEMCHECK_ENABLED=1 for pmemcheck instrumentation,
- LIBPMEMOBJ_CPP_VG_MEMCHECK_ENABLED=1 for memcheck instrumentation,
- LIBPMEMOBJ_CPP_VG_HELGRIND_ENABLED=1 for helgrind instrumentation,
- LIBPMEMOBJ_CPP_VG_DRD_ENABLED=1 for drd instrumentation, or
- LIBPMEMOBJ_CPP_VG_ENABLED=1 for all Valgrind instrumentations (including pmemcheck).

If there are no memcheck / helgrind / drd / pmemcheck headers installed on your
system, build will fail.

## On Windows ##

#### Install libpmemobj via vcpkg ####
```sh
vcpkg install pmdk:x64-windows
vcpkg integrate install
```

```sh
...
cmake . -Bbuild -G "Visual Studio 14 2015 Win64"
        -DCMAKE_TOOLCHAIN_FILE=c:/tools/vcpkg/scripts/buildsystems/vcpkg.cmake

msbuild build/ALL_BUILD.vcxproj
```
