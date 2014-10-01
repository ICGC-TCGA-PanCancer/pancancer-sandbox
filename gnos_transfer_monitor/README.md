# GNOS Transfer Monitor

## Overview

This code will eventaully help us to periodically launch EC2 instances in different regions, one or more per region, download one or more GNOS-hosted BAM files, calculate transfer speed based on runtime, and report this back via a mechanism to a central monitor that plots/summarizes the transfer rates over time.

## Dependencies

* AWS host with .aws config file with credentials
* a worker host AMI per region that will be launched
* gtdownload installed on worker image
* a GNOS pem key
* perl libs TBD

## Running

### Simple GTDownload Timer

This is the most basic component that simply runs and times gtdownload:

    perl time_gtdownload.pl --url <gnos_download_url> --url-file <url-file> --output <output.json> --pem <key_file.pem>

You can use the --url-file in place of --url and include one GNOS download URL per line.

The --output option outputs a summary statistic in JSON format.

## Output

    {
      "gtrep-ebi": {
        "GB": 129349101,
        "time": 1921,
        "GB/s": 5,
        "seconds_since_epoc": 102020101
       },
       "gtrepo-osdc-icgc": {
         "GB": 129349101,
         "time": 1921,
         "GB/s": 5,
         "seconds_since_epoc": 100201012
       }
    }


## TODO

* need a tool that launches other EC2 hosts and runs this command on them
* need a tool to create a url-file from a given GNOS host
