#!/bin/bash
./project2_run > $1 2>&1 &
pidstat -p `pidof project2_run` -r 1 > $1_mem 2>&1 &
pidstat -p `pidof project2_run` -u 1 > $1_cpu 2>&1 &
