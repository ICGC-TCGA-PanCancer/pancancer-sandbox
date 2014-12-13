#!/bin/bash

set -e

# change this as needed
WEB_DIR=$1
if [ -z $WEB_DIR ] || [ ! -d $WEB_DIR ]; then
  echo Must specify an existing web dir through which report data to be exposed! e.g. /var/www/gnos_metadata
  exit 1
fi

DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

echo Script location: $DIR

cd $DIR

# this is just for running individual reports, does not affect downloader download from all gnos repos
gnos_repos=(ebi bsc dkfz)

echo
echo synchronizing with GNOS repos
./gnos_metadata_downloader.py -c settings.yml

echo
echo parsing metadata xml, build ES index

echo parsing all gnos
./parse_gnos_xml.py -c settings.yml

for g in ${gnos_repos[*]};
  do echo parsing gnos $g;
  ./parse_gnos_xml.py -c settings.yml -r $g;
done

echo
echo generating reports
# find the latest folder with metadata
M=`find gnos_metadata -maxdepth 1 -type d -regex 'gnos_metadata/20[0-9][0-9]-[0-9][0-9].*[0-9][0-9]_[A-Z][A-Z][A-Z]' | sort | tail -1`
echo running alignment summary report for $M
for g in ${gnos_repos[*]};
  do
  ./pc_report-donors_alignment_summary.py -m $M -r $g;
done

./pc_report-donors_alignment_summary.py -m $M

echo gzip all jsonl files under $M
gzip $M/*.jsonl

# comment this out for now as it seems cause problems
echo generating aggregated QC prioritization metric
perl ../metadata_tools/prioritise_by_qc.pl $M/donor_p_????????????.jsonl.gz > $M/qc_donor_prioritization.txt


# create symlink
echo
echo updating symlinks

cd $WEB_DIR
DIRNAME=$DIR/$M
DIRNAME=`echo ${DIRNAME##*/}`
if [ -h $DIRNAME ]; then
  rm $DIRNAME
fi
ln -s $DIR/$M $DIRNAME

if [ -h latest ]; then
  rm latest
fi
ln -s $DIRNAME latest

echo finished
echo
