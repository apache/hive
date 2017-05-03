#!/usr/bin/python

#
# This script will search the required Test drivers from the CliConfigs class
# that need to be used to run the tests for the specified q-tests passed on '--paths'.
#

import argparse
import os
import re
import sys

PREFIX_XMLNS = "{http://maven.apache.org/POM/4.0.0}"

POM_HADOOP_VERSION_NAME = "active.hadoop.version"
POM_HADOOP2_VERSION_VALUE = "hadoop-23.version"

# Config classes which need to be ignored in the CliConfigs class
CLASS_NAMES_TO_IGNORE = ["HdfsBlobstoreCliConfig", "PerfCliConfig", "BeeLineConfig", "DummyConfig", "MiniTezCliConfig", "AccumuloCliConfig"]

# A dictionary which contains the name of the test driver class for each config class
DRIVER_FOR_CONFIG_CLASS = {}

# After parsing CliConfigs, an instance of ConfigClass will be created for each config class which
# is defined in CliConfigs. These instances will contain the information, like query and result
# directory and include/exclude list.
class ConfigClass:
    def __init__(self, full_code):
        self.classname = re.findall(r"(\w+)\s+extends\s+AbstractCliConfig\s*{.*", full_code)[0]
        self.includes = re.findall(r"includesFrom\s*\(\s*\w+\s*,\s*\"([\w./]+)", full_code)
        self.excludes= re.findall(r"excludesFrom\s*\(\s*\w+\s*,\s*\"([\w./]+)", full_code)
        self.query_directory = re.findall(r"setQueryDir\s*\(\s*\"([\w./-]+)", full_code)[0]
        self.result_directory = re.findall(r"setResultsDir\s*\(\s*\"([\w./-]+)", full_code)[0]
        self.override_query_file = re.findall(r"overrideUserQueryFile\s*\(\s*\"([\w./-]+)", full_code)

def get_classes(config_file_path):
    all_classes = []
    content = read_java(config_file_path)
    for part in re.split(" class ", content):
        if re.search(r"(\w+) extends AbstractCliConfig", part):
            all_classes.append(ConfigClass(part))

    return all_classes

def read_java(config_file_path):
    with open(config_file_path, "r") as f:
        content = f.read()
        return content
    raise IOError

# Get the driver class - config class mapping from the driver classes
def get_config_class_for_driver(driver_file_path):
    config_class_name = ""
    content = read_java(driver_file_path)
    if re.search(r"(\s+)static\s*CliAdapter\s*adapter", content):
        config_class_name = re.findall(r"\s+CliConfigs\.(\w+)\(\)\.getCliAdapter", content)[0]
    return config_class_name

def load_properties(filepath, sep='=', comment_char='#'):
    """
    Read the file passed as parameter as a properties file.
    """
    props = {}
    with open(filepath, "rt") as f:
        for line in f:
            l = line.strip()
            if l and not l.startswith(comment_char):
                if l.endswith("\\"):
                    l = l.strip("\\")

                key_value = l.split(sep)

                if len(key_value) == 1:
                    props_val = props_val + key_value[0].strip()
                else:
                    props_key = key_value[0].strip()
                    props_val = key_value[1].strip('" \t"')

                props[props_key] = props_val

    return props

def replace_vars(config_keys, props_vars):
    replaced_vars = []
    for key in config_keys:
        if key in props_vars:
            replaced_vars.append(props_vars[key])
        else:
            replaced_vars.append(key)

    return ",".join(replaced_vars)

# Check if a qfile is included in the config class defined in CliConfigs by looking into the following
# attributes:
#   includesFrom:    List of .q files that are run if the driver is executed without using -Dqfile=
#   excludesFrom:    List of .q files that should be excluded from the driver
def is_qfile_include(excludes, includes, qfile, testproperties, override_qfile):

    '''
    Example of a config class in CliConfigs:

    public static class MinimrCliConfig extends AbstractCliConfig {
        public MinimrCliConfig() {
            super(CoreCliDriver.class);
                try {
                    setQueryDir("ql/src/test/queries/clientpositive");
                    includesFrom(testConfigProps, "minimr.query.files");
	                excludesFrom(testConfigProps, "minillap.query.files");
                    setResultsDir("ql/src/test/results/clientpositive");
	...
    }
    '''

    testproperties["qfile"] = qfile

    # Checks if the qfile is not excluded from qtestgen
    if excludes is not None and len(excludes) > 0:
        excluded_files = replace_vars(excludes, testproperties).split(",")
        if qfile in excluded_files:
            return False

    # If includesFrom exists, then check if the qfile is included, otherwise return False
    if includes is not None and len(includes) > 0:
        included_files = replace_vars(includes, testproperties).split(",")
        return qfile in included_files

    # There are some drivers that has queryFile set to a file.
    # i.e. queryFile="hbase_bulk.m"
    # If it is set like the above line, then we should not use such driver if qfile is different
    if override_qfile is not None and len(override_qfile) > 0:
        override_query_file = replace_vars(override_qfile, testproperties).split(",")
        return qfile in override_query_file

    return True

# Search for drivers that can run the specified qfile (.q) by looking into the 'query_directory' attribute
def get_drivers_for_qfile(config_classes, testproperties, qdir, qfile):
    drivers = []
    for config_class in config_classes:
        driver = get_driver_from_config_class(config_class, testproperties, qfile, qdir, config_class.query_directory)
	if driver is not None and len(driver) > 0:
	    drivers.append(driver)

    return drivers

# Search for drivers that can run the specified qfile result (.q.out) by looking into the 'result_directory' attribute
def get_drivers_for_qresults(config_classes, testproperties, qresults, qfile):
    drivers = []
    for config_class in config_classes:
        driver = get_driver_from_config_class(config_class, testproperties, qfile, qresults, config_class.result_directory)
	if driver is not None and len(driver) > 0:
	    drivers.append(driver)

    return drivers

# Get the name of the driver class
def get_driver_from_config_class(config_class, testproperties, qfile, qfile_dir, class_dir):
    driver_name = ""
    if config_class.classname not in CLASS_NAMES_TO_IGNORE and config_class.classname in DRIVER_FOR_CONFIG_CLASS and re.compile(qfile_dir).search(class_dir) is not None:
        if is_qfile_include(config_class.excludes, config_class.includes, qfile, testproperties, config_class.override_query_file):
            driver_name = DRIVER_FOR_CONFIG_CLASS[config_class.classname]

    return driver_name

#
# This command accepts a list of paths (.q or .q.out paths), and displays the
# Test drivers that should be used for testing such q-test files.
#
# The command needs the path to CliConfigs.java to look for the drivers.
#
if __name__ == "__main__":
    # Parse command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("--paths")
    parser.add_argument("--properties")
    parser.add_argument("--hadoopVersion")
    parser.add_argument("--cliConfigsPath")
    parser.add_argument("--driverClassPaths")
    args = parser.parse_args()

    if args.properties is None:
        print "The parameter '--properties' was not found."
        print "Please specify the testconfiguration.properties by using '--propeties <file>"
        sys.exit(1)

    if args.paths is None:
        print "The parameter '--paths' was not found"
        print "Please specify a list of comma separated .q paths (or .q.out paths)"
        sys.exit(1)

    if args.cliConfigsPath is None:
        print "The parameter '--cliConfigsPath' was not found"
        print "Please specify the CliConfig.java file by using '--cliConfigsPath <file>'"
        sys.exit(1)

    if args.driverClassPaths is None:
        print "The parameter '--driverClassPaths' was not found"
        print "Please specify the path of the driver classes by using '--driverClassPaths <file>'"
        sys.exit(1)

    testproperties = load_properties(args.properties)

    testproperties[POM_HADOOP_VERSION_NAME] = POM_HADOOP2_VERSION_VALUE
    if args.hadoopVersion is not None:
        testproperties[POM_HADOOP_VERSION_NAME] = args.hadoopVersion

    # Get all paths information, and get the correct Test driver
    if args.paths:
        tests = {}

        for driver_path in args.driverClassPaths.split(","):
            config_class_name = get_config_class_for_driver(driver_path)
            driver_class_name = os.path.basename(driver_path)
            if config_class_name is not None and len(config_class_name) > 0:
                DRIVER_FOR_CONFIG_CLASS[config_class_name] = driver_class_name[:-5]
            
        config_classes = get_classes(args.cliConfigsPath)

	    # --paths has a list of paths comma separated
        for p in args.paths.split(","):
            dirname = os.path.dirname(p)
            basename = os.path.basename(p)

            # Use a different method to look for .q.out files
            if re.compile("results").search(dirname):
                qfile = basename[0:basename.index(".out")]
                drivers = get_drivers_for_qresults(config_classes, testproperties, dirname, qfile)
            else:
                qfile = basename
                drivers = get_drivers_for_qfile(config_classes, testproperties, dirname, qfile)

            # We make sure to not repeat tests if for some reason we passed something
            # like a.q and a.q.out
            for d in drivers:
                if d in tests:
                    if not qfile in tests[d]:
                        tests[d].append(qfile)
                else:
                    tests[d] = [qfile]

        for t in tests:
            print "%s:%s" % (t, ",".join(tests[t]))
