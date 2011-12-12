#!/usr/bin/env python
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import print_function

import os
import os.path
import re
import base64
import argparse
from xml.dom import Node
from xml.dom.minidom import parseString

from mako.template import Template

report_dir = os.path.dirname(os.path.realpath(__file__))
one_file = False

class TemplateRenderer():
    def __init__(self, template):
        self.__template = template

    def render(self, file_name = None):
        if file_name is None:
            file_name = self.__template
        if not file_name.startswith('/'):
            current_dir = os.path.dirname(os.path.realpath(__file__))
            file_name = os.path.join(current_dir, file_name)
        return Template(filename = file_name).render(this = self)

    def render_link(self, link_name, file_name):
        if one_file:
            return 'data:text/html;charset=utf-8;base64,' + \
                    base64.b64encode(self.render(file_name))
        else:
            part = self.render(file_name)
            with open(report_dir + '/' + link_name, 'w') as f:
                f.write(part)
            return link_name

    def render_files(self):
        report = self.render()
        with open(report_dir + '/report.html', 'w') as f:
            f.write(report)

class TestCase(TemplateRenderer):
    def __init__(self, element):
        TemplateRenderer.__init__(self, 'templates/TestCase.html')

        self.__class_name = element.getAttribute('classname')
        self.__name = element.getAttribute('name')
        self.__time = float(element.getAttribute('time'))
        self.__failure = False
        self.__error = False
        self.__log = None

        for child in element.childNodes:
            if child.nodeType == Node.ELEMENT_NODE and child.tagName == 'failure':
                self.__failure = True
                self.__log = child.firstChild.nodeValue
            elif child.nodeType == Node.ELEMENT_NODE and child.tagName == 'error':
                self.__error = True
                self.__log = child.firstChild.nodeValue

    def success(self):
        return not (self.failure() or self.error())

    def failure(self):
        return self.__failure

    def error(self):
        return self.__error

    def get_log(self):
        return self.__log

    def get_name(self):
        return self.__name

    def get_time(self):
        return self.__time

class TestSuite(TemplateRenderer):
    def __init__(self, text):
        TemplateRenderer.__init__(self, 'templates/TestSuite.html')

        self.properties = {}
        self.test_cases = []

        xml = parseString(text)
        self.__populate_properties(xml)
        self.__populate_test_cases(xml)

        top = xml.getElementsByTagName('testsuite')[0]
        self.__errors = int(top.getAttribute('errors'))
        self.__failures = int(top.getAttribute('failures'))
        self.__tests = int(top.getAttribute('tests'))
        self.__host_name = top.getAttribute('hostname')
        dist_dir = self.properties['dist.dir']
        build_number = re.findall(self.__host_name + '-([0-9]+)$', dist_dir)
        if build_number:
            # Multiple builds per host.
            self.__host_name += '-' + build_number[0]
        self.__name = top.getAttribute('name').split('.')[-1]
        self.__time = float(top.getAttribute('time'))

    def __populate_properties(self, xml):
        properties = xml.getElementsByTagName('property')
        for prop in properties:
            self.properties[prop.getAttribute('name')] = prop.getAttribute('value')

    def __populate_test_cases(self, xml):
        test_cases = xml.getElementsByTagName('testcase')
        for test in test_cases:
            self.test_cases.append(TestCase(test))

    def tests(self):
        return self.__tests

    def failures(self):
        return self.__failures

    def errors(self):
        return self.__errors

    def passes(self):
        return self.tests() - self.failures() - self.errors()

    def time(self):
        return self.__time

    def host_name(self):
        return self.__host_name

    def name(self):
        return self.__name

    def label(self):
        return self.host_name() + '-' + self.name()

class TestRun(TemplateRenderer):
    def __init__(self, pwd):
        TemplateRenderer.__init__(self, 'templates/TestRun.html')

        self.test_suites = []

        files = os.listdir(pwd)
        pattern = re.compile('^TEST-.*\.xml$')
        for f in files:
            if pattern.search(f) is not None:
                with open(os.path.join(pwd, f)) as handle:
                    self.test_suites.append(TestSuite(handle.read()))

    def passes(self):
        return reduce(lambda acc, x: acc + x.passes(), self.test_suites, 0)

    def failures(self):
        return reduce(lambda acc, x: acc + x.failures(), self.test_suites, 0)

    def errors(self):
        return reduce(lambda acc, x: acc + x.errors(), self.test_suites, 0)

    def tests(self):
        return reduce(lambda acc, x: acc + x.tests(), self.test_suites, 0)

    def time(self):
        return reduce(lambda acc, x: acc + x.time(), self.test_suites, 0)

    def success_rate(self):
        if self.tests():
          return 100.0 * self.passes() / self.tests()
        else:
          return 100.0

def make_report(args):
    global report_dir, one_file
    report_dir = args.report_dir
    one_file = args.one_file

    test_run = TestRun(args.log_dir)
    test_run.render_files()
    print('Summary:')
    print('  tests run:', test_run.tests())
    print('  failures:', test_run.failures())
    print('  errors:', test_run.errors())

    failed_results = {}
    files = os.listdir(args.log_dir)
    pattern = re.compile('^([^-]+-[^-]+)-(.*)\.fail$')
    for f in files:
        match = pattern.findall(f)
        if match:
            (host, test, ) = match[0]
            if host not in failed_results:
                failed_results[host] = []
            failed_results[host].append(test)
    if failed_results:
        print()
        print('Some tests faled to produce a log and are not included in the report:')
        for host in failed_results:
            print('  {0}:'.format(host))
            for test in failed_results[host]:
                print('    {0}'.format(test))

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
            description = 'Create HTML report from JUnit logs.')
    parser.add_argument(dest = 'log_dir',
            help = 'Path to directory containing JUnit logs')
    parser.add_argument(dest = 'report_dir',
            help = 'Where should the report be generated')
    parser.add_argument('--one-file', action = 'store_true', dest = 'one_file',
            help = 'Inline everything and generate only one file')
    args = parser.parse_args()

    make_report(args)
