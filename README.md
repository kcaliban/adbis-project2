# ADBIS: Project 2
This repository contains an implementation of:
* Hash join
* Sort-merge join
* Parallel sort-merge join

A complexity analysis and benchmark of these algorithms
can be found in `Project2.pdf`.

## Build instructions
```bash
mkdir build
cd build
cmake -DCMAKE_BUILD_TYPE=Release ..
make
```
## Running benchmarks
Place `100k.txt` and `watdiv.10M.nt` in the `build` directoy.
Run from inside build directory using `./project2_run`.

