import re

# Make a system call:
# curl -u admin@admin.com:admin http://localhost:8080/SeqWareWebService/reports/workflows/34
# Where 34 is the workflow accession id

def main():
    with open('dump') as f:
        data = f.readlines()

    final = {}
    times = {}

    in_step = False
    step = {}
    for line in data:
        match1 = re.search('Processing:\s+(.+)\n', line)
        match2 = re.search('Running time:\s+(.+)\n', line)
        match3 = re.search('SWID:\s+(.+)\n', line)

        if match1 is not None:
            if in_step is False:
                in_step = True
                step['name'] = match1.group(1)
                continue

        if match2 is not None:
            if in_step is False:
                continue
            time = match2.group(1).split(':')
            step['time'] = float(time[0]) * 3600 + float(time[1]) * 60 + float(time[2])

        if match3 is not None:
            if in_step is False:
                continue
            step['swid'] = match3.group(1)
            in_step = False

            if step['name'] in final:
                if int(step['swid']) < int(final[step['name']]['time']):
                    step = {}

            if step != {}:
                final[step['name']] = step
                times[step['name']] = step['time']
                step = {}

    for key, val in times.iteritems():
        print "%s\t%s" % (key, val)


if __name__ == '__main__':
    main()

