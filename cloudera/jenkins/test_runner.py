import logging
import os
import sys
import util
import shutil
import xunitparser


class TestRunner(object):
    """
    Running the specific test on the hive source
    """

    def __init__(self, root_dir, log_dir, commit_id, hadoop_nr):
        logging.debug("TestRunner(%s, %s, %s)", root_dir, log_dir, commit_id)
        self.hadoop_param = "-Phadoop-%s" % hadoop_nr

        if root_dir is None or log_dir is None or commit_id is None:
            logging.error("root_dir, log_dir, commit_id should not be null")
            sys.exit(1)
        self.root_dir = root_dir
        self.log_dir = log_dir
        self.commit_id = commit_id
        self.__prepare_source(True)

    def rollback_source(self, commit_id):
        """
        Rolls back the source to the given commit, and builds the project
        :param commit_id: The commit to roll back to
        :return: -
        """
        logging.debug("rollback_source(%s)", commit_id)
        self.commit_id = commit_id
        self.__prepare_source(False)

    def rerun_test(self, test_case, full_batch):
        """
        Rerun a given testcase
        :param test_case: The testcase to run again
        :param full_batch: If true, then the full batch is run, if false only the single testcase
        :return: The result of the new run
        """
        logging.debug("rerun_test(%s, %s)", test_case, full_batch)

        file_name = test_case.classname.split(".")[-1] + ".java"
        logging.debug("Looking for %s", file_name)
        test_file = util.find(file_name, self.root_dir)
        logging.debug("Found %s", test_file)

        if test_file is None:
            logging.error("Skipping test_case, file_name %s not found", file_name)
            return None

        (module_root, name) = os.path.split(test_file)
        while name and name != "src" and name != "target":
            (module_root, name) = os.path.split(module_root)
        logging.debug("Module root is %s", module_root)

        logging.debug("Removing previous test results")
        shutil.rmtree(os.path.join(module_root, "target", "surefire-reports"), ignore_errors=True)

        if os.path.split(module_root)[-1].startswith("qtest"):
            if not test_case.methodname:
                logging.info("Do not want to replay %s", test_case.classname)
                sys.exit(1)
            command = self.__get_query_test_command(test_case, full_batch)
            new_test_result = self.__run_test(module_root, command, test_case.classname, test_case.methodname)
        else:
            if not test_case.methodname:
                command = self.__get_junit_test_class_command(test_case.classname)
                new_test_result = self.__run_test_class(module_root, command, test_case.classname)
            else:
                command = self.__get_junit_test_command(test_case, full_batch)
                new_test_result = self.__run_test(module_root, command, test_case.classname, test_case.methodname)

        base_dir = os.path.join(self.log_dir, "rerun", test_case.classname + "_" + test_case.methodname)
        test_result_dir = self.__generate_new_test_results_dir_name(base_dir)
        self.__store_test_results(module_root, test_result_dir)
        if new_test_result is not None:
            logging.debug("Test result is %s %s", new_test_result, new_test_result.good)
        else:
            logging.debug("No test result generated")
        return new_test_result

    def __prepare_source(self, clean=True):
        """
        Prepares the source code. Checks out the current revision, and builds the project
        :param clean: If true, then run clean before building the source
        :return: -
        """
        logging.debug("__prepare_source(%s)", clean)

        with util.ChangeDir(self.root_dir):
            command = ["git", "fetch", "--all"]

            util.run_and_wait(command, "Error fetching repository data")

            command = ["git", "checkout", self.commit_id]
            util.run_and_wait(command, "Error checking out hash: " + self.commit_id)

            command = ["mvn", "install", "-Phadoop-2", "-DskipTests"]
            if clean:
                command.insert(1, "clean")
            util.run_and_wait(command, "Error building project")

        with util.ChangeDir(os.path.join(self.root_dir, "itests")):
            command = ["mvn", "install", "-Phadoop-2", "-DskipTests"]
            if clean:
                command.insert(1, "clean")
            util.run_and_wait(command, "Error building project")

    def __get_query_test_command(self, test_case, full_batch):
        """
        Generates a qtest running mvn command
        :param test_case: The testcase to run again
        :param full_batch: Should the test run the full batch or only the test standalone
        :return: The command list
        """
        if full_batch:
            query_list = ""
            for batch_case in test_case.suite:
                query_list += TestRunner.__get_queryfile_from_methodname(batch_case.methodname) + ","
            query_list = query_list[:-1]
        else:
            query_list = TestRunner.__get_queryfile_from_methodname(test_case.methodname)
        return ["mvn", "clean", "test", self.hadoop_param, "-Dtest=" + test_case.classname, "-Dqfile=" + query_list]

    @staticmethod
    def __get_queryfile_from_methodname(methodname):
        """
        Gets the query file name from the method name for 5.8, and 5.9 and above formats
        :param methodname: The test method name
        :return: The query file name
        """
        if "[" in methodname:
            return methodname.split("[")[-1][:-1] + ".q"
        else:
            (first, sep, result) = methodname.partition('_')
            return result + ".q"

    def __get_junit_test_class_command(self, testclass):
        """
        Generates a junit test running mvn command which runs every test in the testclass
        :param testclass: The testcase to run again
        :return: The command list
        """
        return ["mvn", "clean", "test", self.hadoop_param, "-Dtest=" + testclass]

    def __get_junit_test_command(self, test_case, full_batch):
        """
        Generates a junit test running mvn command. If the test_case is a parametrized test then
        run the entire class of tests again, since surefire 2.16 is not able to run parametrized
        test methods
        :param test_case: The testcase to run again
        :param full_batch: Should the test run the full batch or only the test standalone
        :return: The command list
        """
        if full_batch:
            test_list = ""
            for batch_case in test_case.suite:
                test_list += batch_case.classname + ","
            test_list = test_list[:-1]
        else:
            if "[" in test_case.methodname:
                test_list = test_case.classname
            else:
                test_list = test_case.classname + "#" + test_case.methodname
        return ["mvn", "clean", "test", self.hadoop_param, "-Dtest=" + test_list]

    @staticmethod
    def __run_test_command(module_root, command):
        """
        Runs a test, and parses the result
        :param module_root: The root module to run from
        :param command: The command that runs the test
        :return: test_case: The result of the test run, if any
        """
        logging.debug("__run_test_command(%s, %s)", module_root, command)

        with util.ChangeDir(module_root):
            util.run_and_wait(command)
        return util.get_test_results(os.path.join(module_root, "target", "surefire-reports"), False)

    @staticmethod
    def __run_test_class(module_root, command, classname):
        """
        Runs every test in a test class, and parses the result. If there is no result,
        or at least one error, return error. Else return success.
        :param module_root: The root module to run from
        :param command: The command that runs the test
        :param classname: The class name to look for in the results
        :return: test_case[]: The array of the results of the test run, if any
        """
        logging.debug("__run_test_class(%s, %s, %s)", module_root, command, classname)

        every_result = TestRunner.__run_test_command(module_root, command)
        test_case = xunitparser.TestCase(classname, "")
        found = False
        for case in every_result:
            if case.classname.split(".")[-1] == classname:
                if not case.good:
                    test_case.seed(case.result)
                    logging.debug("Found failed case: %s", case)
                    return test_case
                found = True
        if not found:
            logging.debug("Not found any result, so returning failed case")
            test_case.seed('error')
        else:
            logging.debug("Found only successful cases")
            test_case.seed('success')
        return test_case

    @staticmethod
    def __run_test(module_root, command, classname, methodname):
        """
        Runs a specific test, and parses the result
        :param module_root: The root module to run from
        :param command: The command that runs the test
        :param classname: The class name to look for in the results
        :param methodname: The method name to look for in the results
        :return: test_case: The result of the test run, if any
        """
        logging.debug("__run_test(%s, %s, %s, %s)", module_root, command, classname, methodname)

        every_result = TestRunner.__run_test_command(module_root, command)
        for case in every_result:
            if case.classname == classname and case.methodname == methodname:
                return case
        logging.debug("Test produced no result")

    @staticmethod
    def __generate_new_test_results_dir_name(destination_dir):
        """
        Generates a test results dir where the logs could be stored for the give test. It creates a
        new subdirectory in the destination_dir starting with 0
        :param destination_dir: The main directory to store the logs
        :return: The generated new directory
        """
        logging.debug("generate_new_test_results_dir(%s)", destination_dir)

        next_child = 0
        if os.path.exists(destination_dir):
            for subdir_name in os.listdir(destination_dir):
                try:
                    subdir_num = int(subdir_name)
                except ValueError:
                    logging.debug("Unexpected directory name in %s: %s", destination_dir, subdir_name)
                    continue

                if os.path.isdir(os.path.join(destination_dir, subdir_name)) and next_child <= subdir_num:
                    next_child = subdir_num + 1

        new_test_results_dir_name = os.path.join(destination_dir, str(next_child))
        logging.debug("Generated new test results directory name: %s", new_test_results_dir_name)
        return new_test_results_dir_name

    @staticmethod
    def __store_test_results(source_dir, destination_dir):
        """
        Move the test results from the source dir to the destination dir
        :param source_dir: The directory to move from
        :param destination_dir: The directory to move to
        :return: -
        """
        logging.debug("store_test_results(%s, %s)", source_dir, destination_dir)

        if not os.path.exists(destination_dir):
            os.makedirs(destination_dir)
            logging.debug("Directory created %s", destination_dir)

        shutil.move(os.path.join(source_dir, "target", "surefire-reports"), destination_dir)
        logging.debug("Reports moved")

        log_dir = os.path.join(source_dir, "target", "tmp", "log")
        if os.path.exists(log_dir):
            shutil.move(log_dir, destination_dir)
            logging.debug("Logs moved")

        query_out_dir = os.path.join(source_dir, "target", "qfile-results")
        if os.path.exists(query_out_dir):
            for root, dirs, files in os.walk(query_out_dir):
                for name in files:
                    shutil.move(os.path.join(root, name), destination_dir)
            logging.debug("Query results moved")
