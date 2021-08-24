#!/bin/bash

CCAM_IN=/g/data/v14/bjs581/ccam/input
CCAM_OUT=/g/data/v14/bjs581/ccam/output3

python drsnew.py $CCAM_IN/*nc $CCAM_OUT -s 2005 -e 2005 -f 1M -p WINE -m ACCESS1-0 -d AUS-44i -v tasmax --cordex --overwrite
