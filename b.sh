#!/bin/bash
set -eux

for arg in "$@";
do
  declare $arg='1'
done

timing_flag=
if [ -v timing ]; then timing_flag="-DTIMING_DATA";fi

g++ -std=c++20 -g -Wall -Wno-class-memaccess $timing_flag fredbuf-test.cpp -ofredbuf-test