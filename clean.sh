#!/bin/bash

usage() {
    cat <<EOF
Usage: $(basename "$0") [--getlinks] [--tries N]

Runs the full cleaning pipeline:

  1. Cleans EJM data:
       input:  data/ejm.csv
       output: output/excel/ejm, output/discarded/ejm,
               output/academic/ejm, output/verbose/ejm

  2. Cleans AEA data:
       input:  data/aea.csv
       output: output/excel/aea, output/discarded/aea,
               output/academic/aea, output/verbose/aea

  3. Joins cleaned academic data:
       input:  output/academic/ejm/ejm.csv,
               output/academic/aea/aea.csv
       output: output/academic/all/all.csv

  4. Sorts to_admin.csv for easier review

Options:
  --getlinks        attempt to fetch external links during cleaning
  --tries N         number of retries when fetching links
  -h, --help        show this message
EOF
}

if [[ "$1" == "-h" || "$1" == "--help" ]]; then
    usage
    exit 0
fi

/usr/bin/env python3 _clean/clean_ejm.py data/ejm.csv \
    output/excel/ejm output/discarded/ejm output/academic/ejm output/verbose/ejm "$@"

/usr/bin/env python3 _clean/clean_aea.py data/aea.csv \
    output/excel/aea output/discarded/aea output/academic/aea output/verbose/aea "$@"

/usr/bin/env python3 _clean/join.py \
    output/excel/all/all.csv \
    output/excel/aea/aea.csv \
    output/excel/ejm/ejm.csv

/usr/bin/env python3 _clean/join.py \
    output/academic/all/all.csv \
    output/academic/aea/aea.csv \
    output/academic/ejm/ejm.csv

cp output/excel/all/all.csv to_admin.csv

./.sort.sh
