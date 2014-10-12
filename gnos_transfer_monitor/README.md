# GNOS Transfer Monitor

## Overview

This code will eventaully help us to periodically launch EC2 instances in different regions, one or more per region, download one or more GNOS-hosted BAM files, calculate transfer speed based on runtime, and report this back via a mechanism to a central monitor that plots/summarizes the transfer rates over time.

## Dependencies

* AWS host with .aws config file with credentials
* a worker host AMI per region that will be launched
* gtdownload installed on worker image
* a GNOS pem key
* perl libs TBD
* for map generation:
    * "sudo apt-get install libtemplate-perl"

## Running

### Simple GTDownload Timer

This is the most basic component that simply runs and times gtdownload:

    perl time_gtdownload.pl [--url <gnos_download_url>] [--url-file <url-file>] --output <output_report_file> [--output-format <tsv|json>] --pem <key_file.pem> --temp <temp_dir> [--test] [--use-s3] [--test-region <AWS region>]

You can use the --url-file in place of --url and include one GNOS download URL per line in a file, comments start with '#'.

The --output option outputs a summary statistic in JSON/tsv format.

The --use-s3 flag outputs a summary file to: s3://pancancer-site-data/transfer_timing.json old.transfer_timing.json

### Example Run

    perl time_gtdownload.pl --url-file urls.small.txt --output report.json --output-format json --pem ~/key.pem --temp /mnt/tmp --use-s3 --test-region virginia

## Output

    {
      "gtrep-ebi": {
        "GB": 129349101,
        "duration": 1921,
        "start": 1921,
        "stop": 1921,
        "GB/s": 5
       },
       "gtrepo-osdc-icgc": {
         "GB": 129349101,
         "duration": 1921,
         "start": 1921,
         "stop": 1921,
         "GB/s": 5
       }
    }


## TODO

* need a tool that launches other EC2 hosts and runs this command on them
* need a tool to create a url-file from a given GNOS host
