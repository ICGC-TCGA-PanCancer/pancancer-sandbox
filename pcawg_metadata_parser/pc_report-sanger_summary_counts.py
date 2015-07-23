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
        "live_sanger_variant_called_donors", # don't switch order
    ]

    report_dir = init_report_dir(metadata_dir, report_name)
    [dates, metadata_dirs] = get_metadata_dirs(metadata_dir)

    data = [["Date", "To be called", "Called"]]
    counts = []
    today_donors = []

    for ctype in count_types:
    	donor_counts = []
        for md in metadata_dirs:
            donors = set()
    	    file_name_pattern = md + '/reports/gnos_repo_summary/' + ctype + '.*.txt'
            files = glob.glob(file_name_pattern)
            for f in files: donors.update(get_donors(f))
            donor_counts.append(len(donors))
            if len(donor_counts) == len(metadata_dirs):
                today_donors.append(donors)

        counts.append(donor_counts)

    for i, d in enumerate(dates): data.append([d, counts[0][i], counts[1][i]])

    with open(report_dir + '/summary_counts.json', 'w') as o: o.write(json.dumps(data))

    compute_site_report_new(metadata_dir, report_dir, today_donors)

    compute_site_report(metadata_dir, report_dir, today_donors)


def compute_site_report_new(metadata_dir, report_dir, today_donors):
    compute_sites = {
        "aws_ireland": set(),
        "aws_oregon": set(),
        "bsc": set(),
        "dkfz": set(),
        "ebi": set(),
        "etri": set(),
        "oicr": set(),
        "pdc1_1": set(),
        "pdc2_0": set(),
        "tokyo": set(),
        "ucsc": set(),
        "sanger": set(),
        "idash": set()
    }

    get_whitelists(compute_sites)

    site_assigned_donors = set()
    site_summary = {}
    unassigned_uncalled_donors = set()

    for c in compute_sites:
        for d in compute_sites:
            if c == d: continue
            if compute_sites.get(c).intersection(compute_sites.get(d)):
                # log overlap donors issue
                print "WARN: overlap donors found between " + c + " and " + d \
                      + ": " + ", ".join(compute_sites.get(c).intersection(compute_sites.get(d)))  
        if not site_summary.get(c):
            site_summary[c] = {
                'Called': len(compute_sites.get(c).intersection(today_donors[1])),
                'To_be_called': len(compute_sites.get(c).intersection(today_donors[0])),
                'Total': len(compute_sites.get(c))    
            }

        site_assigned_donors.update(compute_sites.get(c))
        # report WARN if the sum of Called and To_be_called not equal Total in site_summary
        if not site_summary[c]['Called'] + site_summary[c]['To_be_called'] == site_summary[c]['Total']:
            print "WARN: donors: " + ", ".join(compute_sites.get(c).difference(today_donors[0]).difference(today_donors[1])) + \
            " found in whitelist are not ready to do Sanger variant calling!!!" 

    site_summary['Unassigned'] = {
        'Called': len(today_donors[1].difference(site_assigned_donors)),
        'To_be_called': len(today_donors[0].difference(site_assigned_donors)),
        'Total': len(today_donors[0].union(today_donors[1]).difference(site_assigned_donors))
    }

    unassigned_uncalled_donors = today_donors[0].difference(site_assigned_donors)

    # today's counts
    with open(report_dir + '/summary_compute_site_counts.json', 'w') as o: o.write(json.dumps(site_summary))

    with open(report_dir + '/unassigned_uncalled_donors.txt', 'w') as o:
        o.write('# Unassigned and uncalled donors\n')
        o.write('# dcc_project_code' + '\t' + 'submitter_donor_id' + '\n')
        o.write('\n'.join(unassigned_uncalled_donors) + '\n') 

    """
    # get all previous days counts
    [dates, metadata_dirs] = get_metadata_dirs(metadata_dir, '2015-03-07')

    site_summary_report = []
    for i, md in reversed(list(enumerate(metadata_dirs))):
        summary_site_count_file = md + '/reports/sanger_summary_counts/summary_compute_site_counts.json'
        if not os.path.isfile(summary_site_count_file): continue

        site_counts = json.load(open(summary_site_count_file))
        site_summary_report.append([dates[i], site_counts])

    with open(report_dir + '/hist_summary_compute_site_counts.json', 'w') as o: o.write(json.dumps(site_summary_report))
    """

def compute_site_report(metadata_dir, report_dir, today_donors):
    compute_sites = {
        "aws_ireland": set(),
        "aws_oregon": set(),
        "bsc": set(),
        "dkfz": set(),
        "ebi": set(),
        "etri": set(),
        "oicr": set(),
        "pdc1_1": set(),
        "pdc2_0": set(),
        "tokyo": set(),
        "ucsc": set(),
        "sanger": set(),
        "idash": set()
    }

    get_whitelists(compute_sites)

    completed_donors = {}
    site_assigned_donors = set()

    for c in compute_sites:
        for d in compute_sites:
            if c == d: continue
            if compute_sites.get(c).intersection(compute_sites.get(d)):
                # log overlap donors issue
                print "WARN: overlap donors found between " + c + " and " + d \
                      + ": " + ", ".join(compute_sites.get(c).intersection(compute_sites.get(d)))
        completed_donors[c] = compute_sites.get(c).intersection(today_donors[1])
        site_assigned_donors.update(completed_donors[c])

    site_not_assigned_donors = today_donors[1].difference(site_assigned_donors)

    #print completed_donors
    #print site_not_assigned_donors

    site_summary = {}
    for c in completed_donors: site_summary[c] = len(completed_donors.get(c))

    # today's counts
    with open(report_dir + '/summary_site_counts.json', 'w') as o: o.write(json.dumps(site_summary))

    # get all previous days counts
    [dates, metadata_dirs] = get_metadata_dirs(metadata_dir, '2015-03-07')

    site_summary_report = []
    for i, md in reversed(list(enumerate(metadata_dirs))):
        summary_site_count_file = md + '/reports/sanger_summary_counts/summary_site_counts.json'
        if not os.path.isfile(summary_site_count_file): continue

        site_counts = json.load(open(summary_site_count_file))
        site_summary_report.append([dates[i], site_counts])
    with open(report_dir + '/hist_summary_site_counts.json', 'w') as o: o.write(json.dumps(site_summary_report))


def get_whitelists(compute_sites):
    whitelist_dir = '../pcawg-operations/variant_calling/sanger_workflow/whitelists/'

    for c in compute_sites:
        files = glob.glob(whitelist_dir + '/' + c + '/' + c + '.*.txt')
        for f in files: compute_sites.get(c).update(get_donors(f))


def get_donors(fname):
    donors = []
    with open(fname) as f:
        for d in f:
            donors.append(d.rstrip())
    return donors


def get_metadata_dirs(metadata_dir, start_date='2015-01-11'):
    dirs = sorted(glob.glob(metadata_dir + '/../20*_???'))
    dir_name = os.path.basename(metadata_dir)
    ret_dirs = []
    ret_dates = []
    start = False
    for d in dirs:
    	if '../' + start_date in d: start = True
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
