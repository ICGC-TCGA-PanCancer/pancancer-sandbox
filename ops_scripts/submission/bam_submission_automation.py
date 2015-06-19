#!/usr/bin/python

# This quick and dirty script automates the process of taking a multilane bam file
# splitting it apart, reheadering it, and submitting it to GNOS
# It has some command line dependencies, based on the SOP found at:
# https://wiki.oicr.on.ca/display/PANCANCER/PCAWG+%28a.k.a.+PCAP+or+PAWG%29+Sequence+Submission+SOP+-+v1.0

import datetime
import time
import sys
import re
import os
import uuid

PARSER='.analysis_id.(.*?)..analysis_id.'


def SplitBAM(filename):
    basename = '.'.join(filename.split('.')[:-1])
    if not os.path.exists(basename):
        os.mkdir(basename)
    command = ("bamtofastq exclude=SECONDARY,SUPPLEMENTARY,QCFAIL "
               "outputperreadgroup=1 outputdir=%s filename=%s" %
               (basename, filename))
    print command
    os.system(command)


def AssembleFQ(path, normalid):
    path = basename = '.'.join(path.split('.')[:-1])
    header = {}
    header['RGID']="OICR:"
    header['RGCN']="OICR"
    header['RGPL']="ILLUMINA"
    header['RGLB']="WGS:OICR:99"
    header['RGPI']="500"
    header['RGSM']=str(uuid.uuid4())
    header['RGPU']="OICR:999_9"
    header['RGDT']=str(datetime.datetime.now().isoformat('T'))
    header['RGPM']="Illumina HiSeq 2000"
    info = {}
    info["dcc_project_code"]="PACA-CA"
    info["submitter_donor_id"]="blahblah_79797"
    info["submitter_specimen_id"]="blah_specimen_979797"
    info["submitter_sample_id"]="PD3851a7979"
    info["dcc_specimen_type"]="Primary tumour - solid tissue"
    if normalid is None:
        info["use_cntl"]="N/A"
    else:
        info["use_cntl"]=normalid
    
    os.chdir(path)
    files = os.listdir('.')
    filtered = []
    
    # Filter out the indexes
    for f in files:
        match1 = re.search('.*o1.*', f)
        match2 = re.search('.*info.*', f)
        if match1 or match2:
            continue
        filtered.append(f)
    
    files = sorted(filtered)
    commands = []
    
    alternate = False
    count = 9
    for f1, f2 in zip(files[:-1], files[1:]):
        count +=1
        if alternate:
            alternate = False
            continue
        match = re.match('(.*?\.\d)_\d.fq', f1)
        if match is None:
            raise ValueError(f1)
        output_basename = match.group(1)
        # Prepare command and output to stdout
        command1 = ("fastqtobam I=%s I=%s md5=1 md5filename=%s.bam.md5 "
                   "RGID='%s' "
                   "RGCN='%s' "
                   "RGPL='%s' "
                   "RGLB='%s' "
                   "RGPI='%s' "
                   "RGSM='%s' "
                   "RGPU='%s' "
                   "RGDT='%s' "
                   "> %s.bam") % (
            f1, f2, output_basename,
            header['RGID']+"%s" % (count),
            header['RGCN'],
            header['RGPL'],
            header['RGLB'],
            header['RGPI'],
            header['RGSM'],
            header['RGPU'],
            header['RGDT'],
            output_basename
        )
        with open(output_basename+'.bam.info','w') as f:
            for key, value in info.iteritems():
                f.write('%s:%s\n' % (key, value))
            f.write('PM:%s\n' %  header['RGPM'])
        alternate = True
        commands.append((command1, output_basename))
    
    if not os.path.exists("bam"):
        os.mkdir('bam')
    for tup in commands:
        command1, filename = tup
        print command1
        os.system(command1)
        try:
            os.rename(filename+'.bam', os.path.join('bam', filename+'.bam'))
            os.rename(filename+'.bam.info', os.path.join('bam', filename+'.bam.info'))
            os.rename(filename+'.bam.md5', os.path.join('bam', filename+'.bam.md5'))
        except:
            pass
    os.chdir('bam')
    command = "/opt/bin/bam_to_sra_sub.pl -g https://gtrepo-osdc-icgc.annailabs.com -o sra *.bam -s CGTEST"
    os.system(command)


def main(filename):
    
    # First Download the file
    SplitBAM(sys.argv[1])
    
    # Split into fast
    
    # Organize the fastq files and reheader
    
    if len(sys.argv) < 3:
        uuid = None
    else:
        uuid = sys.argv[2]
    AssembleFQ(sys.argv[1], uuid)

    
if __name__ == '__main__':
    if len(sys.argv) < 2:
        print "USASGE: assemble.py bamfiletosplit uuid-of-normal-sample"
        sys.exit(1)
    main(sys.argv[1])

