import urllib2

sitelist = []
urllist = []
#urllist.append("http://pancancer.info/gnos_metadata/latest/analysis_objects.dkfz.tsv")
#sitelist.append("dkfz")
#urllist.append("http://pancancer.info/gnos_metadata/latest/analysis_objects.ebi.tsv")
#sitelist.append("ebi")
#urllist.append("http://pancancer.info/gnos_metadata/latest/analysis_objects.etri.tsv")
#sitelist.append("etri")
urllist.append("http://pancancer.info/gnos_metadata/latest/analysis_objects.osdc-icgc.tsv")
sitelist.append("osdc")

parsed = {}

def main():
    for url in urllist:
        data = urllib2.urlopen(url).readlines()
        for line in data:
            uuid, state, date = line.split('\t')
            if uuid in parsed:
                parsed[uuid] += 1
            else:
                parsed[uuid] = 1
    
    for key, value in parsed.iteritems():
        print key, value
        
        
if __name__ == '__main__':
    main()
