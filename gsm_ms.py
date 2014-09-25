#!/usr/bin/env python

import subprocess, optparse, os
import pyrap.tables as pt
import numpy as np

usage = "usage: python %prog [options] dataset.MS outfile"
description="Generate sky model directly from a measurement set."
vers="2.1"

#Version 2.1 notes
# Added option to remove Cas and Cyg A.

#Version 2.0 notes
# Just switched to using numpy.

#Defines the options as present in gsm.py
parser = optparse.OptionParser(usage=usage, version="%prog v{0}".format(vers), description=description)
parser.add_option("-r", "--radius", action="store", type="float", dest="radius", default=5., help="Specify cone radius in degrees [default: %default]")
parser.add_option("-c", "--cutoff", action="store", type="float", dest="cutoff", default=0.1, help="Choose the minimum flux of VLSS sources to use [default: %default]")
parser.add_option("-t", "--assoctheta", action="store", type="float", dest="assoctheta", default=0.00278, help="Uncertainty in matching in degrees [default: %default (10 arcsec)]")
parser.add_option("-A", "--removeAteam", action="store_true", dest="removeAteam", default=False, help="Removes Cyg A and Cas A from the model [default: %default]")
(options, args) = parser.parse_args()

#gsm.py outfile RA DEC radius [vlssFluxCutoff [assocTheta]]

def clean(f):
	input_model=open(f+".temp", 'r')
	output_model=open(f, 'w+r')
	source_names=[]
	for line in input_model:
		source=str(line)
		source_name=source[:11]
		# print source_name
		if source_name not in source_names:
			if options.removeAteam:
				if source_name in Ateam:
					print "A team source {0} removed".format(source_name)
					continue
			source_names.append(source_name)
			output_model.write(source)
		else:
			print "Source {0} was doubled".format(source_name)
	input_model.close()
	output_model.close()
	print "Cleaned sky model {0} produced".format(f)
	subprocess.call("rm {0}.temp".format(f), shell=True)


Ateam=["2323.2+5850", "2323.4+5849", "1959.4+4044"]
ms=args[0]
outfile=args[1]
rad=options.radius
cut=options.cutoff
asth=options.assoctheta

print "Obtaining RA and Dec of MS..."
obs = pt.table(ms + '/FIELD', ack=False)
ra = np.degrees(float(obs.col('REFERENCE_DIR')[0][0][0]))
if ra < 0.:
	ra=360.+(ra)
dec = np.degrees(float(obs.col('REFERENCE_DIR')[0][0][1]))
print "RA:{0}\tDec:{1}".format(ra, dec)
obs.close()
obs.close()

subprocess.call("gsm.py {0}.temp {1} {2} {3} {4} {5}".format(outfile, ra, dec, rad, cut, asth), shell=True)

clean(outfile)
