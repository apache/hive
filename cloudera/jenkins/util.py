import os
import sys
import logging
import subprocess
import xunitparser


class ChangeDir(object):
    """ Context Manager to change current directory. """
    def __init__(self, newPath):
        self.newPath = os.path.expanduser(newPath)

    def __enter__(self):
        """
        Change directory with the new path
        :return: -
        """
        self.savedPath = os.getcwd()
        os.chdir(self.newPath)

    def __exit__(self, etype, value, traceback):
        """
        Return back to previous directory
        :param etype: Not used
        :param value: Not used
        :param traceback: Not used
        :return:
        """
        os.chdir(self.savedPath)


def find(name, directory):
    """
    Find the given file in the given directory recursively
    :param name: The file to look for
    :param directory: The root directory
    :return: The full path
    """
    for root, dirs, files in os.walk(directory):
        if name in files:
            return os.path.join(root, name)


def run_and_wait(command, error_message=None):
    """
    Executing the specific shell command and waiting for the results. If error_message is set
    then sys.exit() in case of failure with the same code as the command
    :param command: The command to run
    :param error_message: The message to print in case of failure before exit
    :return:
    """
    logging.debug("run_command(%s)", command)
    process = subprocess.Popen(command)
    result = process.wait()
    if result != 0 and error_message is not None:
        logging.error(error_message)
        sys.exit(result)
    return result


def get_test_results(result_dir, only_failed):
    """
    Returns the list of the testcases in the given directory
    :param result_dir: The directory where the junit xml-s are sitting
    :param only_failed: Collect only failed results
    :return: the list of the failed test_cases
    """
    logging.debug("get_test_results(%s, %s)", result_dir, only_failed)

    result = []
    for root, dirs, files in os.walk(result_dir):
        for filename in files:
            logging.debug("Checking file: %s/%s", root, filename)
            if filename.endswith(".xml"):
                test_suite, test_result = xunitparser.parse(open(os.path.join(root, filename)))
                for test_case in test_suite:
                    if not test_case.good or not only_failed:
                        logging.info("Test case data to process: %s %s", test_case.classname, test_case.methodname)
                        test_case.suite = test_suite
                        result.append(test_case)
    return result
