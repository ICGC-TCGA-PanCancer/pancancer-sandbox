#!/bin/bash

set -e

# change this as needed
WEB_DIR=/var/www

DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

echo Script location: $DIR

cd $DIR

# this is just for running individual reports, does not affect downloader download from all gnos repos
gnos_repos=(ebi bsc dkfz etri riken cghub osdc-icgc)

echo
echo synchronizing with GNOS repos
./gnos_metadata_downloader.py -c settings.yml

echo
echo parsing metadata xml, build ES index

for g in ${gnos_repos[*]};
  do echo parsing gnos $g;
  ./parse_gnos_xml.py -c settings.yml -r $g;
done

echo parsing all gnos
./parse_gnos_xml.py -c settings.yml

echo
echo generating reports
# find the latest folder with metadata
M=`find gnos_metadata -regex 'gnos_metadata/20[0-9][0-9]-[0-9][0-9].*[0-9][0-9]_[A-Z][A-Z][A-Z]' | sort | tail -1`
echo running alignment summary report for $M
for g in ${gnos_repos[*]};
  do
  ./pc_report-donors_alignment_summary.py -m $M -r $g;
done

./pc_report-donors_alignment_summary.py -m $M

# create symlink
echo
echo updating symlinks

cd $WEB_DIR
ln -s $DIR/$M .

rm latest
ln -s $DIR/$M latest

echo finished

