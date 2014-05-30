#!/usr/bin/env python

import optparse, subprocess, time, os, sys
from multiprocessing import Pool

usage = "usage: python %prog [options] html.txt"
description="Script to quickly fetch data from the LTA using wget. Currently does not handle corrupted files but will obtain missing files."
vers="1.0"

parser = optparse.OptionParser(usage=usage, version="%prog v{0}".format(vers), description=description)
parser.add_option("-c", "--check-attempts", action="store", type="int", dest="attempts", default=10, help="How many attempts to fetch missing files [default: %default].")
parser.add_option("-d", "--delay", action="store", type="int", dest="delay", default=120, help="Time between each fetch attempt [default: %default].")
parser.add_option("-n", "--njobs", action="store", type="int", dest="ncpus", default=5, help="How many to attempt to fetch at once [default: %default].")
(options, args) = parser.parse_args()

def countdown(wait):
	"""Funky countdown timer"""
	for remaining in range(wait, 0, -1):
	    sys.stdout.write("\r")
	    sys.stdout.write("Will attempt to fetch files in {:02d} seconds...".format(remaining)) 
	    sys.stdout.flush()
	    time.sleep(1)

def fetch(file):
	"""Simple wget get line"""
	print "Fetching {0}...".format(file.split("/")[-1])
	subprocess.call("wget {0} > 2>&1".format(file), shell=True)
	
workers=Pool(processes=options.ncpus)

#read in all the html files the user wishes
files=args[:]
initfetch=[]
for file in files:
	f=open(file, 'r')
	initfetch+=[i.rstrip('\n') for i in f]
	f.close()

#perform initial fetch of all data
workers.map(fetch, initfetch)

#loop to check for missing files from the list
for j in range(options.attempts):
	print "----------------------------------------------------------------------------------------"
	print "Running Missing File Check {0} of {1}".format(j+1, options.attempts)
	tofetch=[k for k in initfetch if not os.path.isfile(k.split('lofigrid/')[-1].replace('/', '%2F'))]
	if len(tofetch) < 1:
		print "0 files remaining"
		print "All files obtained!"
		break
	else:
		print "{0} files to remain to fetch"
		countdown(options.delay)
		workers.map(fetch, tofetch)