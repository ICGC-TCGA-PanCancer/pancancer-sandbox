# Parser code for downloading/parsing/analyzing/aggregating GNOS XMLs

## Overview

The script takes a list of GNOS metadata XML file as input, one XML per GNOS
Analysis Object. It extracts most important information out for each AO (ie, a
BAM entry), then create its associated donor entry and/or specimen entry if one
does not yet exist. The parser produces two JSONL files, one is organized at
donor leve, the other at BAM level. These JSON docs are also pushed to
Elasticsearch for easy search/browse later.

## Dependencies

Python and a few modules: python-dateutil, elasticsearch, xmltodict etc. Install
them with pip when needed.

Elasticsearch installed and up running on the same host using port 9200.

Kibana, simply download it from Elasticsearch website, then run an HTTP server
over the Kibana folder to serve the static content.

GNOS metadata XML files need to be retrieved from all PCAWG GNOS servers and
kept in one single folder. This can usually done by a script wrapping GNOS
cgquery client tool.

## Run the GNOS downloader/synchronizer

```
./gnos_metadata_downloader.py -c settings.yml
```

## Run the parser/ES loader

```
./parse_gnos_xml.py -c settings.yml
```

In addition to build an ES index name as 'p_\<time_stamp\>', two JSONL
files will also be created.

## Run the report generator
```
M=`find gnos_metadata -maxdepth 1 -type d -regex 'gnos_metadata/20[0-9][0-9]-[0-9][0-9].*[0-9][0-9]_[A-Z][A-Z][A-Z]' | sort | tail -1`
./pc_report-donors_alignment_summary.py -m  $M
```

## Run QC prioritization metric generator (Perl script from Keiran)
```
perl ../metadata_tools/prioritise_by_qc.pl $M/donor_p_????????????.jsonl.gz > $M/qc_donor_prioritization.txt
```

## Alternatively run everything at once:
```
./run_me.sh /var/www/gnos_metadata
```

## Getting Pre-Built Indexes

You can find nightly pre-built indexes for local usage here:

    http://pancancer.info/gnos_metadata/
