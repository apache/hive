#!/usr/local/bin/python

import argparse
import sys
import subprocess
import logging
import random
import shutil
import os
import test_runner
import result_writer
import xunitparser
import util
import urllib2
import json
import re

MAX_FAILURE_TO_ANALYZE_COUNT = 3
FLAKY_TEST_DECISION_COUNT = 3
FLAKY_TEST_RUN_COUNT = 5
FAILED_TEST_CHECK_BEHIND_COUNT = 10


class Analyzer(object):
    def __init__(self, source_dir, data_dir, build_url, hadoop):
        self.hadoop = hadoop
        logging.debug("Analyzer(%s, %s, %s)", source_dir, data_dir, build_url)
        self.source_dir = source_dir
        self.data_dir = data_dir
        self.build_url = build_url
        self.source_hash = self.__get_build_hash()

        self.__prepare_data_dir()
        self.result_writer = result_writer.ResultWriter(self.data_dir, self.build_url)

        self.__download_data()
        self.failed_test_cases = self.__get_test_cases()
        self.test_runner = test_runner.TestRunner(self.source_dir, self.data_dir, self.source_hash, hadoop)

    def __get_test_cases(self):
        """
        Get and normalize the test cases
        :return: the array of the failed test cases
        """
        failed_tests = util.get_test_results(self.data_dir, True)
        failed_classes = self.__get_missing_test_cases()
        """ Collect the test cases where the whole class failed (setup problem) and add them to the
        failed test classes. Good for removing duplicates and generalizing these TestCase objects
        """
        failed_classes.extend(
            [test_case.classname.split(".")[-1] for test_case in failed_tests if
             test_case.classname == test_case.methodname and test_case.classname.split(".")[-1] not in failed_classes])
        failed_classes = list(set(failed_classes))
        """ Remove instances of failed classes form the tests if any """
        failed_tests[:] = \
            [test_case for test_case in failed_tests if
             test_case.classname.split(".")[-1] not in failed_classes]
        """ Add the testcase instance to the failed_tests list generated from the failed_classes """
        for failed_class in failed_classes:
            test_case = xunitparser.TestCase(failed_class, "")
            test_case.seed('error')
            failed_tests.append(test_case)

        """ Print out the found test cases """
        logging.debug("Found failed test cases:")
        for failed_test_case in failed_tests:
            logging.debug(" - %s %s", failed_test_case.classname, failed_test_case.methodname)
        return failed_tests

    def analyze(self):
        """ Analyzes the test results """
        logging.debug("analyze()")
        """
        Check if there is many test failures, and select MAX_FAILURE_TO_TRACK to check only those
        """
        tracked_test_cases = self.failed_test_cases
        if len(self.failed_test_cases) > MAX_FAILURE_TO_ANALYZE_COUNT:
            tracked_test_cases = random.sample(self.failed_test_cases, MAX_FAILURE_TO_ANALYZE_COUNT)
            logging.info("Number of testcases is bigger than maximum (%s vs. %s)",
                         len(self.failed_test_cases), MAX_FAILURE_TO_ANALYZE_COUNT)
            logging.info("Tracking the following:")
            for case in tracked_test_cases:
                logging.info(" - %s %s", case.classname, case.methodname)

        failures = []
        for case in tracked_test_cases:
            """ Runs the test again for FLAKY_TEST_DECISION_COUNT times """
            found_success = False
            found_failure = False
            for _ in xrange(0, FLAKY_TEST_DECISION_COUNT):
                checked_test_case = self.test_runner.rerun_test(case, False)
                if checked_test_case is None:
                    logging.info("Could not find failed testcase in source %s %s", case.classname, case.methodname)
                    sys.exit(1)
                if checked_test_case.good:
                    found_success = True
                else:
                    found_failure = True
                if found_failure and found_success:
                    """ If found goof and bad results too """
                    self.__analyze_flaky(case)
                    break

            if found_success and not found_failure:
                """
                If all of the runs are successful run it in the same group when the failure happened
                """
                found_group_success = False
                for _ in xrange(0, FLAKY_TEST_DECISION_COUNT):
                    checked_test_case = self.test_runner.rerun_test(case, True)
                    if checked_test_case.good:
                        """ If there is at least a good result then the test is flaky """
                        found_group_success = True
                        self.__analyze_flaky(case)
                        break

                if not found_group_success:
                    """ If every group run failed then report a group run error """
                    group = []
                    for group_case in case.suite:
                        group.append((group_case.classname, group_case.methodname))
                    self.result_writer.feedback_group_error(case.classname, case.methodname, group)

            if found_failure and not found_success:
                """
                If the run is still failing then mark it to check later together with every
                failing tests
                """
                failures.append(case)

        """ If there are any failure the check which commit is to blame """
        if len(failures) > 0:
            self.__analyze_failures(failures)

        """ If the run is finished remove the badge from the parent build """
        """ we need a username and a password for it to work :( """
        """ util.run_and_wait(["wget", args.build + "/parent/parent/plugin/groovy-postbuild/removeBadges"], "Error removing badge") """
        self.result_writer.finalize_result()

    def __prepare_data_dir(self):
        """
        Cleans up the previous results and creates the data directory
        :return: The data directory to use
        """
        logging.debug("__prepare_data_dir()")

        logging.debug("Removing previous downloaded data")
        shutil.rmtree(self.data_dir, ignore_errors=True)
        os.makedirs(self.data_dir)

    def __download_data(self):
        """
        Downloads the test results to the data directory
        :return: -
        """
        logging.debug("__download_data()")

        with util.ChangeDir(self.data_dir):
            util.run_and_wait(["wget", self.build_url + "/artifact/*zip*/archive.zip"], "Error downloading test results")
            util.run_and_wait(["unzip", "archive.zip"], "Error uncompressing test results")
            util.run_and_wait(["wget", self.build_url + "/consoleText"], "Error downloading console text")

    def __get_missing_test_cases(self):
        """
        Calulcates the missing test_cases using the console output
        :return: the list of the missed test_cases
        """
        logging.debug("__get_missing_test_cases()")

        result = []
        missed = r".* - ([\S]+) - did not produce a TEST-\*.xml file"
        console_file_name = os.path.join(self.data_dir, "consoleText")
        with open(console_file_name) as console_file:
            for line in console_file:
                match = re.search(missed, line)
                if match:
                    if match.group(1) not in result:
                        result.append(match.group(1))
        if result:
            logging.info("Found testcase(s) with missing result Test-*.xml: %s", result)
        else:
            logging.info("No missing testcase found")
        return result

    def __get_build_hash(self):
        """
        Downloads the build parameters using the jenkins python api, and gets the git hash of the
        last commit
        :return: hash of the build
        """
        logging.debug("__get_build_hash()")

        response = urllib2.urlopen(self.build_url + "/api/python")
        raw = response.read().decode("utf-8")
        raw = raw.replace(":False", ":\"False\"")
        raw = raw.replace(":True", ":\"True\"")
        raw = raw.replace(":None", ":\"None\"")
        logging.debug("Downloaded raw: %s", raw)
        data = json.loads(raw)
        for action in data['actions']:
            if 'lastBuiltRevision' in action:
                found_hash = action['lastBuiltRevision']['SHA1']
                logging.info("Found hash: %s", found_hash)
                return found_hash
        logging.info("Git hash not found")
        sys.exit(1)

    def __analyze_flaky(self, test_case):
        """
        Analyze a flaky tests by running it FLAKY_TEST_RUN_NUMBER times, and reporting the statistics
        :param test_case: The testcase to run again
        :return: The list of the results of the new runs
        """
        logging.debug("__analyze_flaky(%s)", test_case)

        new_results = []
        flaky_successes = 0
        flaky_failures = 0
        for _ in xrange(0, FLAKY_TEST_RUN_COUNT):
            test_result = self.test_runner.rerun_test(test_case, False)
            new_results.append(test_result)
            if test_result.good:
                flaky_successes += 1
            else:
                flaky_failures += 1

        self.result_writer.feedback_flaky_response(test_case.classname, test_case.methodname, flaky_failures, flaky_successes)
        return new_results

    def __analyze_failures(self, test_case_array):
        """
        Analyze a failed tests by checking out previous versions of the code and running it again
        until all of the is successful, or FAILED_TEST_MAX_CHECK_NUMBER is reached. Reporting the
        commit_id when the test is failed the first time, or if when the test is first appeared in the
        code
        :param test_case_array: The array of the failed testcases to run again
        :return: The list of the results of the new runs
        """
        logging.debug("__analyze_failure(%s)", test_case_array)

        last_commit_ids = self.__get_last_commit_ids(FAILED_TEST_CHECK_BEHIND_COUNT)
        bad_commit_id = last_commit_ids.pop(0)
        for commit_id in last_commit_ids:
            logging.debug("Analyzing %s commit", commit_id)
            self.test_runner.rollback_source(commit_id)
            for test_case in test_case_array:
                """ Run the tests which are still failing """
                test_result = self.test_runner.rerun_test(test_case, False)
                if test_result is None or test_result.good:
                    """ If the test was successfull remove from the list and report the commit """
                    test_case_array.remove(test_case)
                    log = self.__get_commit_log(bad_commit_id)
                    self.result_writer.feedback_failed_response(test_case.classname, test_case.methodname, bad_commit_id, log)
                    return
                bad_commit_id = commit_id

        for test_case in test_case_array:
            self.result_writer.feedback_no_result(test_case.classname, test_case.methodname)
        return

    def __get_last_commit_ids(self, max_number):
        """
        Return the last commit ids for the given git repository
        :param max_number: The maximum number of the result list
        :return:
        """
        logging.debug("__get_commit_list(%s)", max_number)

        last_commit_ids = []
        with util.ChangeDir(self.source_dir):
            try:
                text = subprocess.check_output(["git", "log", "--pretty=format:%H", "-n", str(max_number)])
                for line in text.splitlines():
                    last_commit_ids.append(line)
            except subprocess.CalledProcessError, e:
                logging.debug("Error while trying to get the commit list: %s", e)
        logging.debug("Commit list generated %s", last_commit_ids)
        return last_commit_ids

    def __get_commit_log(self, commit_id):
        """
        Get the commit log data for the given commit id
        :param commit_id: The commitId we are looking for
        :return: The commit log information string
        """
        logging.debug("__get_commit_log(%s)", commit_id)
        with util.ChangeDir(self.source_dir):
            try:
                command = ["git", "log", "-1", "--format=fuller", commit_id]
                logging.debug("Executing: %s", command)
                return subprocess.check_output(command)
            except subprocess.CalledProcessError, e:
                logging.error("Error while trying to get the commit data: %s", e)
                sys.exit(1)


#
# This command runs the failure analyzis described in CDH-48184
#
if __name__ == "__main__":
    """ Parse command line arguments """
    parser = argparse.ArgumentParser()
    parser.add_argument("--build")
    parser.add_argument("--log")
    parser.add_argument("--source")
    parser.add_argument("--hadoop")
    args = parser.parse_args()

    if args.log is not None:
        numeric_level = getattr(logging, args.log.upper(), None)
        if not isinstance(numeric_level, int):
            raise ValueError("Invalid log level: %s", args.log)
        logging.basicConfig(level=numeric_level)
    else:
        logging.basicConfig(level=logging.INFO)

    if args.build is None:
        print "The parameter '--build' was not found."
        print "Please specify the jenkins build Url with '--build <url>' like --build http://unittest.jenkins.cloudera.com/job/Hive-Post-Commit-For-Test/42"
        sys.exit(1)
    else:
        if args.build[-1] == '/':
            args.build = args.build[:-1]

    if args.hadoop is None:
        args.hadoop = 2
    else:
        if not args.hadoop.isdigit():
            print "The parameter '--hadoop' is not an integer."
            sys.exit(1)


    """ Used for testing purposes """
    source = "."
    if args.source is not None:
        source = args.source

    analyzer = Analyzer(source, os.path.join(source, "tmp"), args.build, args.hadoop)
    analyzer.analyze()

    print "\n\n\n\n\nResults:"
    with open(os.path.join(os.path.join(source, "tmp"), result_writer.RESULT_TXT_FILE), "r") as result_file:
        print result_file.read()
