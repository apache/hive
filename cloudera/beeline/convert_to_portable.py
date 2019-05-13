#!/usr/bin/python

#
# This script will create the portable versions of the q.out files.
# The goal is to ensure that the query results are not changing between CDH versions.
# This script removes the commands, and the outputs of these commands which are contain
# changes not relevant to our checks. Like: Explain commands, describe commands,
# show commnads
# It is possible to run the script for a single file (--file), or a directory (--dir)
# Examples:
#    - ./convert_to_portable --file query.q.out
#    - ./convert_to_portable --dir ql/src/test/results
#

import argparse
import re
import sys
import os

COMMANDS_TO_REMOVE = [
      "EXPLAIN",
      "DESC(RIBE)?[\s\\n]+EXTENDED",
      "DESC(RIBE)?[\s\\n]+FORMATTED",
      "DESC(RIBE)?",
      "SHOW[\s\\n]+TABLES",
      "SHOW[\s\\n]+FORMATTED[\s\\n]+INDEXES",
      "SHOW[\s\\n]+DATABASES"]

def convert_file(file):
    print "Converting file: %s" % file
    with open(file, "r") as source:
        data = source.read()
    for command in COMMANDS_TO_REMOVE:
        pattern = "(?is)PREHOOK: query:\s+%s[\\n\s]+.*?(?=(PREHOOK: query:|$))" % command
        data = re.sub(pattern, "", data)
    with open(file + ".portable", "w") as result:
        result.write(data)

if __name__ == "__main__":
    """ Parse command line arguments """
    parser = argparse.ArgumentParser()
    parser.add_argument("--file",
                        type=str,
                        help="Query out file to convert")
    parser.add_argument("--dir",
                        default="../../ql/src/test/results",
                        type=str,
                        help="The directory which contains the query out files to convert")
    args = parser.parse_args()

    if args.file and args.dir:
        print "Please provice only --dir, or --file"
        sys.exit(1)

    if not args.file and not args.dir:
        print "Please provice one of the following --dir, or --file"
        sys.exit(1)

    if args.file:
        convert_file(args.file)

    if args.dir:
        for root, dirs, files in os.walk(args.dir):
            for name in files:
                convert_file(os.path.join(root, name))


