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

# update the code
echo update the code
git pull

echo update pcawg-operations git submodule where white lists are maintained
cd ../pcawg-operations/
git pull
cd $DIR

# this is just for running individual reports, does not affect downloader download from all gnos repos
#gnos_repos=(ebi bsc dkfz)

echo
echo synchronizing with GNOS repos
./gnos_metadata_downloader.py -c settings.yml


echo
echo cleaning up older ES indexes
for f in `curl 'localhost:9200/_cat/indices?v' |awk '{print $3}' |grep p_ |grep -v p_150210030103 |sort -r |tail -n +3`;
  do echo deleting index $f ;
  curl -XDELETE localhost:9200/$f ;
done


echo
echo parsing metadata xml, build ES index

echo parsing all gnos
./parse_gnos_xml.py -c settings.yml -x gnos_ids_to_be_removed.tsv

# update ES alias to point to the latest index
echo 'delete old alias'
for f in `curl 'localhost:9200/_cat/aliases?v' |grep pcawg_es |awk '{print $2}'`;
  do echo deleting alias for $f ;
  curl -XDELETE localhost:9200/$f/_alias/pcawg_es ;
done

echo 'create new alias'
f=`curl 'localhost:9200/_cat/indices?v' |awk '{print $3}' |grep p_ |sort |tail -1`
curl -XPUT localhost:9200/$f/_alias/pcawg_es


#for g in ${gnos_repos[*]};
#  do echo parsing gnos $g;
#  ./parse_gnos_xml.py -c settings.yml -r $g;
#done

echo
echo generating reports
# find the latest folder with metadata
M=`find gnos_metadata -maxdepth 1 -type d -regex 'gnos_metadata/20[0-9][0-9]-[0-9][0-9].*[0-9][0-9]_[A-Z][A-Z][A-Z]' | sort | tail -1`
echo running alignment summary report for $M
#for g in ${gnos_repos[*]};
#  do
#  ./pc_report-donors_alignment_summary.py -m $M -r $g;
#done

# now report on compute site
./pc_report-donors_alignment_summary.py -m $M
./pc_report-gnos_repo_summary.py -m $M
./pc_report-summary_counts.py -m $M
./pc_report-sanger_call_missing_input.py -m $M
./pc_report-donors_RNA_Seq_alignment_summary.py -m $M

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
