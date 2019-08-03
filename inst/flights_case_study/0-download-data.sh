#!/bin/bash

cd inst/flights_case_study/data/

for i in {1987..2008}; do wget -q http://stat-computing.org/dataexpo/2009/"$i".csv.bz2; done

for i in {1987..2008}; do bunzip2 "$i".csv.bz2;  done
