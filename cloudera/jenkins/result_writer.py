import os
import logging

RESULT_HTML_FILE = "results.html"
RESULT_TXT_FILE = "results.txt"
RESULT_SUMMARY_FILE = "summary.html"


class ResultWriter(object):
    """ Object for rendering an analyzis result """

    def __init__(self, root_dir, build_url):
        logging.debug("ResultWriter(%s, %s)", root_dir, build_url)
        self.html_file = os.path.join(root_dir, RESULT_HTML_FILE)
        self.txt_file = os.path.join(root_dir, RESULT_TXT_FILE)
        self.summary_file = os.path.join(root_dir, RESULT_SUMMARY_FILE)
        with open(self.html_file, "a") as result_file:
            result_file.write("Analyzis is started for <a href='%s'>%s</a>" % (build_url, build_url))
        with open(self.txt_file, "a") as result_file:
            result_file.write("Analyzis is started for %s\n\n" % build_url)
        with open(self.summary_file, "a") as result_file:
            result_file.write("Analyzis of <a href='%s'>%s#%s</a>\n" % (build_url, build_url.split("/")[-2], build_url.split("/")[-1]))

    def finalize_result(self):
        """ Print the closing lines """
        logging.debug("finalize_result()")
        with open(self.html_file, "a") as result_file:
            result_file.write("<br/>Analyzis successful")
        with open(self.txt_file, "a") as result_file:
            result_file.write("Analyzis successful")

    def feedback_flaky_response(self, classname, method, failures, successes):
        """
        Handles the feedback for the failed tests
        :param classname: The flaky test classname
        :param method: The flaky test method, or query test runner and query
        :param failures: The number of failures during testing
        :param successes: The number of successful runs during testing
        :return: -
        """
        logging.debug("feedback_flaky_response(%s, %s, %s, %s)", classname, method, failures, successes)

        logging.info("Testcase %s %s flaky test analyzis resulted in %s/%s failures/successes",
                     classname, method, failures, successes)

        with open(self.html_file, "a") as result_file:
            result_file.write("<br/><b>%s - %s is flaky</b> (%s/%s - failure/success)\n" % (classname, method, failures, successes))
        with open(self.txt_file, "a") as result_file:
            result_file.write("%s - %s is flaky (%s/%s - failure/success)\n" % (classname, method, failures, successes))
        with open(self.summary_file, "a") as result_file:
            result_file.write("<br/>Flaky %s#%s\n" % (classname.split(".")[-1], method))

    def feedback_failed_response(self, classname, method, commit_id, commit_log):
        """
        Handles the feedback for the flaky tests
        :param classname: The flaky test classname
        :param method: The flaky test method, or query test runner and query
        :param commit_id: The commit hash to blame
        :param commit_log: The commit log
        :return: -
        """
        logging.debug("feedback_failed_response(%s, %s, %s, %s)", classname, method, commit_id, commit_log)

        logging.info("Testcase %s %s failed %s commit is blamed", classname, method, commit_id)
        logging.info("Commit:\n\n%s", commit_log)

        with open(self.html_file, "a") as result_file:
            result_file.write("<br/><b>%s - %s is broken by %s</b><br/><pre>%s</pre>\n" % (classname, method, commit_id, commit_log))
        with open(self.txt_file, "a") as result_file:
            result_file.write("%s - %s is broken by %s\n%s\n" % (classname, method, commit_id, commit_log))
        with open(self.summary_file, "a") as result_file:
            result_file.write("<br/>Broken %s#%s by %s\n" % (classname.split(".")[-1], method, commit_id))

    def feedback_group_error(self, classname, method, group):
        """
        Handles the feedback for group errors
        :param classname: The failed test classname
        :param method: The failed test methodname
        :param group: The list of the classname, methodname of the group causing the failure
        :return: -
        """
        logging.debug("feedback_group_error(%s, %s, %s)", classname, method, group)

        logging.info("Testcase %s %s failed in the following group", classname, method)
        for (test_class, test_method) in group:
            logging.info(" - %s %s", test_class, test_method)

        result_html_file = open(self.html_file, "a")
        result_txt_file = open(self.txt_file, "a")
        result_summary_file = open(self.summary_file, "a")

        result_html_file.write("<br/><b>%s - %s is broken if run in this group<br/><ul>\n" % (classname, method))
        result_txt_file.write(">%s - %s is broken if run in this group\n" % (classname, method))
        result_summary_file.write("<br/>Group error %s#%s\n" % (classname.split(".")[-1], method))
        for (test_class, test_method) in group:
            result_html_file.write("<li>%s %s</li>\n" % (test_class, test_method))
            result_txt_file.write("- %s %s\n" % (test_class, test_method))
        result_html_file.write("</ul>\n")
        result_txt_file.write("\n")

    def feedback_no_result(self, classname, method):
        """
        Handles the feedback for the failures where no commit could be blamed
        :param classname: The failed test classname
        :param method: The failed test methodname
        :return: -
        """
        logging.debug("feedback_no_result(%s, %s)", classname, method)

        logging.info("Was not able to find the commit to blame for testcase %s %s", classname, method)

        with open(self.html_file, "a") as result_file:
            result_file.write("<br/><b>%s - %s is broken</b> but no braking commit found\n" % (classname, method))
        with open(self.txt_file, "a") as result_file:
            result_file.write("%s - %s is broken but no braking commit found\n" % (classname, method))
        with open(self.summary_file, "a") as result_file:
            result_file.write("<br/>Pass on %s#%s\n" % (classname.split(".")[-1], method))
