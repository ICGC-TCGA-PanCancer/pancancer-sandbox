#!/usr/bin/env python

import os
import sys
import csv
import json
import shutil
import datetime
from subprocess import call, check_output
from pprint import pprint as Dumper
from glob import glob

def day(delta):
    """Get the formatted date of today or yesterday"""
    if delta:
        then = datetime.datetime.today() - datetime.timedelta(delta)
        return then.strftime('%Y-%m-%d')
    else:
        return datetime.datetime.today().strftime('%Y-%m-%d')

# Shh, don't tell anyone -- for reconstructing history, to retrofit JSON changes
num1 = sys.argv[1] if len(sys.argv) > 1 and sys.argv[1] else 0
num2 = sys.argv[2] if len(sys.argv) > 2 and sys.argv[2] else 1

today = day(int(num1))
yesterday = day(int(num2))
print "Today is "+today

def add_count(counts,category,pcode,count):
    """Adds donor counts by p[roject and category"""
    if category not in counts.keys():
        counts[category] = {}
    counts[category][pcode] = count

def call_and_check(command):
    """wraps system call with retval check"""
    assert len(command) > 0, "command must not be empty!"
    print "Making system call for '"+command+"'"
    retval = call(command, shell=True)
    assert retval == 0, "Failure executing command."

def save_json(count_file, data):
    """Saves count data to a json file"""
    print "Saving JSON file "+count_file
    cwd = os.getcwd()
    os.chdir(today_path)
    json_string = json.dumps(data)
    with open(count_file, 'w') as outfile:
        outfile.write(json_string + "\n")
    file_test(count_file)
    os.chdir(cwd)
    return count_file

def file_test(count_file):
    """Sanity check for empty or non-exsistent results"""
    assert os.path.isfile(count_file), 'Error: '+count_file+' does not exists.'
    assert os.stat(count_file).st_size > 0,'Error: '+count_file+' is empty.'

def add_timepoint(timing,thing,repo,day,value):
    """Adds time point to timing data by operation/repo"""
    assert timing.keys() > 0, "This is not the timing hash I expected"
    if day not in timing[thing].keys():
        timing[thing][day] = {}
    if repo not in timing[thing][day].keys():
        timing[thing][day][repo] = []
    timing[thing][day][repo].append(value)

def add_type_count(counts,pclass,pinstance):
    """Counts files counts by project"""
    if pclass not in counts.keys():
        counts[pclass] = {}
    if pinstance not in counts[pclass].keys():
        counts[pclass][pinstance] = 0
    counts[pclass][pinstance] += 1

def add_item(projects,project):
    """Keeps track of master project list"""
    if project not in projects.keys():
        projects[project] = 1


git_base       = '/mnt/data/s3-transfer-operations'

# Update data from github
print "Updating s3 transfer data..."
cwd = os.getcwd()
os.chdir(git_base)
if int(num1) == 0:
    call_and_check("git checkout master")
    call_and_check("git pull")
else:
    call_and_check("git checkout 'master@{"+today+" 23:59:59}'")

# More than one cluster (currently no)?
clusters = ['/s3-transfer-jobs']
# clusters.append ['/s3-transfer-jobs-2']

for path in clusters:
    git_path       = git_base + path
    reports        = git_path.replace('transfer-jobs','metadata')
    timing_path    = git_base + '/timing-information'
    today_path     = reports  + '/' + today
    yesterday_path = reports  + '/' + yesterday

    if os.path.exists(today_path):
        try:
            shutil.rmtree(today_path)
        except:
            sys.stderr.write("I seem to have a problem removing "+today_path)
        try:
            os.unlink(reports+'/latest')
        except:
            sys.stderr.write("I seem to have a problem getting rid of symlink "+reports+"/latest")
        
    call_and_check("ls -al "+reports)

    os.makedirs(today_path)

    print "Mapping GNOS repos and IDs..."
    os.chdir(git_path)
    gnos_id_to_repo = {}
    all_repos = {}
    json_files = glob('*/*.json')
    for jfile in json_files:
        with open(jfile) as json_file:
            try:
                json_data = json.load(json_file)
            except:
                sys.stderr.write("problem parsing "+git_path+"/"+jfile+"!\n")
                continue
        gnos_id   = json_data.get('gnos_id')
        gnos_url  = json_data.get('gnos_repo')
        if gnos_id:
            gnos_url  = str(gnos_url[0])
            gnos_repo = gnos_url.replace('https://gtrepo-','')
            gnos_repo = gnos_repo.replace('.annailabs.com/','')
            gnos_id_to_repo[gnos_id] = gnos_repo
            add_item(all_repos,gnos_repo)
                
    # Get the count for each queueing category
    print "Getting file counts..."
    total_file_count = {}
    file_count_project = {}
    file_count_repo = {}
    all_projects = {}
    jobs = glob('*jobs')
    for status in jobs:
        files = glob(status+'/*.json')
        count = len(files)
        total_file_count[status] = count
        for file in files:
            pcode = file.split('.')[1]
            gnos_id = file.split('.')[0]
            gnos_id = gnos_id.split('/')[1]
            #print "GNOS ID is "+gnos_id
            gnos_repo = gnos_id_to_repo.get(gnos_id)
            add_item(all_projects,pcode)
            add_type_count(file_count_project,status,pcode)
            add_type_count(file_count_repo,status,gnos_repo)

    save_json('project_counts.json',file_count_project)
    save_json('repo_counts.json',file_count_repo)

    # Get/save project and repo lists
    hist_json = []
    yest_json = yesterday_path + '/projects.json'
    if os.path.isfile(yest_json):
        with open(yest_json) as json_file:
            hist_json = json.load(json_file)

    if len(hist_json) > 0:
        for proj in hist_json:
            add_item(all_projects,proj)
        
    projects = all_projects.keys()
    projects.sort()
    save_json('projects.json',projects)

    hist_json = []
    yest_json = yesterday_path + '/repos.json'
    if os.path.isfile(yest_json):
        with open(yest_json) as json_file:
            hist_json = json.load(json_file)

    if len(hist_json) > 0:
        for proj in hist_json:
            add_item(all_repos,proj)

    repos = all_repos.keys()
    repos.sort()
    save_json('repos.json',repos)


    # Get/save count history data
    print "Getting count history..."
    hist_json = {}
    yest_json = yesterday_path + '/hist_counts.json'
    if os.path.isfile(yest_json):
        with open(yest_json) as json_file:
            hist_json = json.load(json_file)

    hist_json[today] = total_file_count
    #Dumper(hist_json)
    save_json('hist_counts.json',hist_json)

    # Get timing information
    print "Getting timing data..."
    timing = {'download':{},'upload':{}}
    os.chdir(timing_path)
    files = glob('*.json.timing')
    for timing_file in files:
        with open(timing_file) as infile:
            csv_rows = csv.reader(infile)
            for row in csv_rows:
                if row[0] == 'gnos-id':
                    continue

                gnos_id = row[0]
                gnos_repo = gnos_id_to_repo.get(gnos_id)
                if not gnos_repo:
                    break

                nums = [float(t) for t in list(row)[1:12]]
                (download_start,
                 download_stop,
                 download_delta,
                 upload_start,
                 upload_stop,
                 upload_delta,
                 workflow_start,
                 workflow_stop,
                 workflow_delta,
                 download_size,
                 upload_size) = nums

                download_day = datetime.datetime.fromtimestamp(download_start).strftime('%Y-%m-%d')
                download_rate = (download_size/download_delta)/1024
                add_timepoint(timing,'download',gnos_repo,download_day,download_rate)
                upload_day = datetime.datetime.fromtimestamp(upload_start).strftime('%Y-%m-%d')
                upload_rate = (upload_size/upload_delta)/1024
                add_timepoint(timing,'upload',gnos_repo,upload_day,upload_rate)


    save_json('timing.json',timing)

    right_now = [datetime.datetime.today().strftime('%H:%M UTC %A, %B %d')]
    Dumper(right_now)
    save_json('timestamp.json',right_now)

    os.chdir('/var/www/gnos_metadata/latest/reports/s3_transfer_summary/')
    files = glob('*.txt')

    counts = {}
    pcodes = {}
    for file in files:
        columns = file.split('.')
        pcode = columns[0]
        category = columns[1]
        donors = sum(1 for line in open(file)) - 1
        add_count(counts,category,pcode,donors)
        pcodes[pcode] = 1
        
    projects = pcodes.keys()
    projects.sort()
    columns = counts.keys()
    columns.sort()
        
    rows = []
    header = []
    header.append('Project')
    for category in columns:
        column = category.replace('_', ' ')
        column = column.replace('transferred ', 'transferred, ')
        header.append(column)
    header.append('Total')
    rows.append(header)
            
    column_total = {'Total':0}
    for project in projects:
        row = [project]
        row_total = 0
        for category in columns:
            count_hash = counts[category]
            if project not in count_hash.keys():
                count_hash[project] = 0
            row.append(count_hash[project])
            row_total += count_hash[project]
            if category not in column_total.keys():
                column_total[category] = 0
            column_total[category] += count_hash[project]
        row.append(row_total)
        column_total['Total'] += row_total
        rows.append(row)

    row = ['Total']
    columns.append('Total');
    for category in columns:
        row.append(column_total[category])
        print "Appending "+category+" "+str(column_total[category]);
    rows.append(row)

    save_json('s3_transfer_status.json',rows)

    os.chdir(reports)
    call_and_check("ln -sf "+today+" latest")