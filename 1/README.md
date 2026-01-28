Corobus (Task 1)
================

Implements a coroutine-friendly message bus with bounded channels, including
broadcast and batch send/recv.

Quick Start
-----------
Build and run tests:

```sh
mkdir -p build
cd build
cmake ..
make
./test
```


Files
-----
- `corobus.h` — public API and behavior contracts
- `corobus.cpp` — implementation
- `test.cpp` — main test suite
- `libcoro_test.cpp` — coroutine engine tests
