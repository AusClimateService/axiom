#!/bin/bash

set -ex

CCAM_IN=/datastore/raf018/CCAM/WINE/ACCESS1-0/50km/cordex
CCAM_OUT=/scratch1/sch576/ccam/output

# For testing
CCAM_OLD=/datastore/raf018/CCAM/WINE/ACCESS1-0/50km/DRS/CORDEX/output/AUS-44i/CSIRO/CSIRO-BOM-ACCESS1-0/rcp85/r1i1p1/CSIRO-CCAM-1704/v1/mon/tasmax/tasmax_AUS-44i_CSIRO-BOM-ACCESS1-0_rcp85_r1i1p1_CSIRO-CCAM-1704_v1_mon_201901-201912.nc

CCAM_NEW=/scratch1/sch576/ccam/output/WINE/output/AUS-44i/CSIRO/CSIRO-BOM-ACCESS1-0/rcp85/r1i1p1/CSIRO-CCAM-1704/v1/mon/tasmax/tasmax_AUS-44i_CSIRO-BOM-ACCESS1-0_rcp85_r1i1p1_CSIRO-CCAM-1704_v1_mon_201901-201912.nc

python drsnew.py $CCAM_IN/*2019*nc $CCAM_OUT -r 50 -s 2019 -e 2019 -f 1M -p WINE -m ACCESS1-0 -d AUS-44i -v tasmax --cordex --overwrite

# Difference the fields
cdo sub $CCAM_OLD $CCAM_NEW diff.nc

# Run a diff on the metadata, ignoring things that would be different anyway
axiom diff $CCAM_OLD $CCAM_NEW --ignore _global:creation_date,_global:tracking_id
