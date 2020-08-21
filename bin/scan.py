import sys, os, fileinput
from lxml import etree

XPATH = "/foxml:digitalObject/foxml:datastream[@CONTROL_GROUP='E']/foxml:datastreamVersion/foxml:contentLocation[contains(@REF, '%s')]/../.."
NS={'foxml': 'info:fedora/fedora-system:def/foxml#'}
def scan(sourcepath, fragment):
	doc = etree.parse(sourcepath)
	res = doc.xpath(XPATH % fragment, namespaces=NS)
	if len(res) > 0:
		for datastream in set(res):
			mostRecent = sorted(datastream.getchildren(), key=lambda node: node.get('CREATED'))[-1]
			contentLocation = mostRecent.xpath('foxml:contentLocation', namespaces=NS)[0].get('REF')
			if fragment in contentLocation:
				values = [datastream.getparent().get('PID'), datastream.get('ID'), ('"%s"' % contentLocation), mostRecent.get('CREATED')]
				print(",".join(values))
	del doc
def main():
	if sys.argv[1] == None:
		print("no path fragment given to match against", file=sys.stderr)
		sys.exit(1)
	print("PID,DSID,PATH,TIMESTAMP")
	# read file paths as lines from stdin/pipe
	for line in fileinput.input('-'):
		try:
			scan(line.strip(), sys.argv[1])
		except:
			print("BROKEN,BROKEN,\"%s\",0000-00-00T00:00:00.000Z" % line.strip())
if __name__ == "__main__":
	main()