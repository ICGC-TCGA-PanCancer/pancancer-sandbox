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

