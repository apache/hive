#!/usr/bin/python
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from BeautifulSoup import BeautifulSoup
import urllib2
import xmltodict
import json
import Queue
from threading import Thread
from collections import OrderedDict
import itertools
from ascii_graph import Pyasciigraph
import sys
import argparse
import os

# default build that is used against apache hive precommit test report
REPORTS_DIR = "/tmp/slow-test-reports"
BUILD_NUMBER = 830
TOP_K = 25
json_dumps = []

# parallel xml report downloader
class ReportDownloader(Thread):
	def __init__(self, q):
		Thread.__init__(self)
		self.q = q

	def run(self):
		while True:
			# Get the work from the queue and expand the tuple
			link = self.q.get()
			xmlFile = urllib2.urlopen(link)
			xmlData = xmlFile.read()
			xmlSoup = BeautifulSoup(xmlData)
			d = xmltodict.parse(xmlData, xml_attribs=True)
			d['testsuite'].pop('properties', None)
			json_dumps.append(d)
			self.q.task_done()

def get_links(rootUrl):
	html_page = urllib2.urlopen(rootUrl)
	soup = BeautifulSoup(html_page)
	result = []
	for link in soup.findAll('a'):
		hrefs = link.get('href')
		if hrefs.endswith('.xml'):
			result.append(rootUrl + "/" + hrefs)
	
	return result

def take(iterable, n=TOP_K):
    return list(itertools.islice(iterable, 0, n))

def plot_testsuite_time(json_data, top_k=TOP_K, ascii_graph=False, report_file=None):
	suite_time = {}
	
	overall_time = 0.0
	for suite in json_data:
		name = suite['testsuite']['@name'].rsplit(".",1)[-1]
		time = float(suite['testsuite']['@time'].replace(',',''))
		overall_time += time
		if name in suite_time:
			total_time = suite_time[name]
			suite_time[name] = total_time + time
		else:
			suite_time[name] = time

	d_descending = OrderedDict(sorted(suite_time.items(), 
                                  key=lambda kv: kv[1], reverse=True))

	gdata = []
	for k,v in take(d_descending.iteritems(), top_k):
		gdata.append((k, v))

	print '\nTop ' + str(top_k) + ' testsuite in terms of execution time (in seconds).. [Total time: ' + str(overall_time) + ' seconds]'
	if ascii_graph:
		graph = Pyasciigraph()
		for line in  graph.graph('', gdata):
			print line
	else:
		for line in gdata:
			print line[0] + "\t" + str(line[1])

		if report_file != None:
			with open(report_file, "w") as f:
				f.write('Top ' + str(top_k) + ' testsuite in terms of execution time (in seconds).. [Total time: ' + str(overall_time) + ' seconds]\n')
				for line in gdata:
					f.write(line[0] + "\t" + str(line[1]) + "\n")


def plot_testcase_time(json_data, top_k=TOP_K, ascii_graph=False, report_file=None):
	testcase_time = {}
	
	overall_time = 0.0
	for suite in json_data:
		if int(suite['testsuite']['@tests']) > 0:
			for t in suite['testsuite']['testcase']:
				if isinstance(t, dict):
					name = t['@classname'].rsplit(".",1)[-1] + "_" + t['@name']
					time = float(t['@time'].replace(',',''))
					overall_time += time
					if name in testcase_time:
						total_time = testcase_time[name]
						testcase_time[name] = total_time + time
					else:
						testcase_time[name] = time
		if int(suite['testsuite']['@tests']) == 0:
			print "Empty batch detected for testsuite: " + suite['testsuite']['@name'] + " which took " + suite['testsuite']['@time'] + "s"
				
	d_descending = OrderedDict(sorted(testcase_time.items(), 
                                  key=lambda kv: kv[1], reverse=True))

	gdata = []
	for k,v in take(d_descending.iteritems(), top_k):
		gdata.append((k, v))


	print '\nTop ' + str(top_k) + ' testcases in terms of execution time (in seconds).. [Total time: ' + str(overall_time) + ' seconds]'
	if ascii_graph:
		graph = Pyasciigraph()
		for line in  graph.graph('', gdata):
			print line
	else:
		for line in gdata:
			print line[0] + "\t" + str(line[1])

		if report_file != None:
			with open(report_file, "a") as f:
				f.write('\nTop ' + str(top_k) + ' testcases in terms of execution time (in seconds).. [Total time: ' + str(overall_time) + ' seconds]\n')
				for line in gdata:
					f.write(line[0] + "\t" + str(line[1]) + "\n")

def get_latest_build_with_report(build_number):
	latest_report = BUILD_NUMBER
	if not os.path.exists(REPORTS_DIR):
		os.makedirs(REPORTS_DIR)
	for i in os.listdir(REPORTS_DIR):
		if i.endswith(".txt"):
			current_report = int(i.split(".txt")[0])
			if current_report > latest_report:
				latest_report = current_report

	return latest_report

def get_pending_report_list(last_report, precommit_url):
	next_report = last_report
	pending_reports = []
	done = False
	while done == False:
		try:
			urllib2.urlopen(precommit_url % next_report)
			pending_reports.append(next_report)
			next_report += 1
		except urllib2.HTTPError, e:
			done = True

	return pending_reports

def print_report(reportUrl, json_dump, top_k, ascii_graph, report_file=None):
	get_links(reportUrl)
	links = get_links(reportUrl)
	# Create a queue to communicate with the worker threads
	q = Queue.Queue()
	print "\nProcessing " + str(len(links)) + " test xml reports from " + reportUrl + ".."
	# Create 8 worker threads
	for x in range(8):
 		worker = ReportDownloader(q)
 		# Setting daemon to True will let the main thread exit even though the workers are blocking
 		worker.daemon = True
 		worker.start()

 	# Put the tasks into the queue as a tuple
 	for link in links:
 		q.put(link)
 	
 	# Causes the main thread to wait for the queue to finish processing all the tasks
	q.join()

	# dump test reports in json format
	if json_dump:
		with open('data.json', 'w') as outfile:
			json.dump(json_dumps, outfile, indent = 2)

	# print or plot top-k tests on console
	plot_testsuite_time(json_dumps, top_k, ascii_graph, report_file)
	plot_testcase_time(json_dumps, top_k, ascii_graph, report_file)
	del json_dumps[:]

def main():
	parser = argparse.ArgumentParser(description='Program to print top-k test report for Apache Hive precommit tests')
	parser.add_argument('-b', action='store', dest='build_number', help='build number of the test run. default uses test reports from apache hive precommit test run.')
	parser.add_argument('-u', action='store', dest='report_url', help='url for the test report')
	parser.add_argument('-j', action='store_true', default=False, dest='json_dump', help='json dump of test reports')
	parser.add_argument('-k', action='store', dest='top_k', type=int, help='print top k testsuite and testcases to console')
	parser.add_argument('-a', action='store_true', default=False, dest='ascii_graph', help='ascii output of the report')
	parser.add_argument('-l', action='store_true', default=False, dest='latest_report', help='will generate all missing reports up until latest build number')
	args = parser.parse_args()

        precommit_url = "http://104.198.109.242/logs/PreCommit-HIVE-Build-%s/test-results/"
	last_report = get_latest_build_with_report(BUILD_NUMBER)
	pending_reports = get_pending_report_list(last_report, precommit_url)

	build = last_report
	if args.build_number != None:
		build = args.build_number

	reportUrl = precommit_url % build
	if args.report_url != None:
		reportUrl = args.report_url

	json_dump = args.json_dump

	top_k = TOP_K
	if args.top_k != None:
		top_k = args.top_k

	ascii_graph = args.ascii_graph

	print_report(reportUrl, json_dump, top_k, ascii_graph, REPORTS_DIR + str(build) + ".txt")

	if args.latest_report:
		for l in pending_reports:
			reportUrl = precommit_url % l
			print_report(reportUrl, json_dump, top_k, ascii_graph, REPORTS_DIR + str(l) + ".txt")

main()
