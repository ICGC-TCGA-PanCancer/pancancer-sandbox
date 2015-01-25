#!/usr/bin/env python

import sys
import os
import glob
import json
import re
from argparse import ArgumentParser
from argparse import RawDescriptionHelpFormatter


def init_report_dir(metadata_dir, report_name):
    report_dir = metadata_dir + '/reports/' + report_name
    if not os.path.exists(report_dir):
        os.makedirs(report_dir)
    return report_dir


def generate_report(metadata_dir, report_name):
    count_types = [
        "live_aligned_sanger_variant_not_called_donors",
        "live_sanger_variant_called_donors",
    ]

    report_dir = init_report_dir(metadata_dir, report_name)
    [dates, metadata_dirs] = get_metadata_dirs(metadata_dir)

    data = [["Date", "Not Called", "Called"]]
    counts = []

    for ctype in count_types:
    	donor_counts = []
        for md in metadata_dirs:
    	    file_name_pattern = md + '/reports/gnos_repo_summary/' + ctype + '.*.txt'
            files = glob.glob(file_name_pattern)
            donor_counts.append(sum([file_len(fname) for fname in files]))
        counts.append(donor_counts)

    for i, d in enumerate(dates):
        data.append([d, counts[0][i], counts[1][i]])

    with open(report_dir + '/summary_counts.json', 'w') as o:
        o.write(json.dumps(data))


def file_len(fname):
    with open(fname) as f:
        for i, l in enumerate(f):
            pass
    return i + 1

def get_metadata_dirs(metadata_dir, start_date='2015-01-11'):
    dirs = sorted(glob.glob(metadata_dir + '/../20*_???'))
    dir_name = os.path.basename(metadata_dir)
    ret_dirs = []
    ret_dates = []
    start = False
    for d in dirs:
    	if start_date in d: start = True
    	if not start: continue
    	ret_dates.append( str.split(os.path.basename(d),'_')[0] )
        ret_dirs.append(d)
        if dir_name == os.path.basename(d): break

    return [ret_dates, ret_dirs]


def main(argv=None):
    if argv is None:
        argv = sys.argv
    else:
        sys.argv.extend(argv)

    parser = ArgumentParser(description="PCAWG Report Generator Gathering Counts",
             formatter_class=RawDescriptionHelpFormatter)
    parser.add_argument("-m", "--metadata_dir", dest="metadata_dir",
             help="Directory containing metadata manifest files", required=True)

    args = parser.parse_args()
    metadata_dir = args.metadata_dir  # this dir contains gnos manifest files, will also host all reports

    if not os.path.isdir(metadata_dir):  # TODO: should add more directory name check to make sure it's right
        sys.exit('Error: specified metadata directory does not exist!')

    report_name = re.sub(r'^pc_report-', '', os.path.basename(__file__))
    report_name = re.sub(r'\.py$', '', report_name)

    generate_report(metadata_dir, report_name)

    return 0


if __name__ == "__main__":
    sys.exit(main())
