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

import argparse
import time
from threading import Thread
import os.path
import collections
import re
import os

import Report
import config

# WARNING
#
# If you are editing this code, please be aware that commands passed to `run`
# should not use single quotes, this will break and end badly as the final
# command looks like `ssh 'host' 'some command - single quote will break it'`.
# Also please be aware that `run` uses `.format` to change `{host}` in commands
# into actual host name it is running on, running `.format` on strings using
# `{host}`, for example including `host_code_path` will not work.
#
# Also this code assumes master_base_path is available to all testing machines
# and is mounted in the same place on all of them.
#
# Getting rid of this restrictions without making the code much more complicated
# is very welcome.

# This is configured in user configuration file.

local = None
qfile_set = None
other_set = None
remote_set = None
all_set = None

master_base_path = None
host_base_path = None
runtest_dir = os.getcwd()

# End of user configurated things.

ant_path = None
arc_path = None
phutil_path = None
code_path = None
report_path = None
host_code_path = None

def read_conf(config_file):
    global local, qfile_set, other_set, remote_set, all_set
    global master_base_path, host_base_path
    global ant_path, arc_path, phutil_path, code_path, report_path, host_code_path

    if config_file is not None:
        config.load(config_file)
    else:
        config.load()

    local = config.local
    qfile_set = config.qfile_set
    other_set = config.other_set
    remote_set = config.remote_set
    all_set = config.all_set

    master_base_path = config.master_base_path
    host_base_path = config.host_base_path

    if 'HIVE_PTEST_SUFFIX' in os.environ:
        suffix = os.environ['HIVE_PTEST_SUFFIX']
        master_base_path += '-' + suffix
        host_base_path  += '-' + suffix

    ant_path = master_base_path + '/apache-ant-1.8.4'
    arc_path = master_base_path + '/arcanist'
    phutil_path = master_base_path + '/libphutil'
    code_path = master_base_path + '/trunk'
    report_path = master_base_path + '/report/' + time.strftime('%m.%d.%Y_%H:%M:%S')
    host_code_path = host_base_path + '/trunk-{host}'

    # Setup of needed environmental variables and paths

    # Ant
    all_set.add_path(ant_path + '/bin')

    # Arcanist
    all_set.add_path(arc_path + '/bin')

    # Java
    all_set.export('JAVA_HOME', config.java_home)
    all_set.add_path(config.java_home + '/bin')

    # Hive
    remote_set.export('HIVE_HOME', host_code_path + '/build/dist')
    remote_set.add_path(host_code_path + '/build/dist/bin')

def get_ant():
    # Gets Ant 1.8.4 from one of Apache mirrors.
    print('\n-- Installing Ant 1.8.4\n')

    if local.run('test -d "{0}"'.format(ant_path), warn_only = True,
            abandon_output = False) is None:
        local.run('mkdir -p "{0}"'.format(master_base_path))
        local.cd(master_base_path)
        local.run('curl "http://apache.osuosl.org//ant/binaries/apache-ant-1.8.4-bin.tar.gz" | tar xz')
    else:
        print('\n  Ant 1.8.4 already installed\n')

def get_arc():
    # Gets latest Arcanist and libphtuil from their Git repositories.
    print('\n-- Updating Arcanist installation\n')

    if local.run('test -d "{0}"'.format(arc_path), warn_only = True,
            abandon_output = False) is None:
        local.run('mkdir -p "{0}"'.format(os.path.dirname(arc_path)))
        local.run('git clone git://github.com/facebook/arcanist.git "{0}"'
                .format(arc_path))

    if local.run('test -d "{0}"'.format(phutil_path), warn_only = True,
            abandon_output = False) is None:
        local.run('mkdir -p "{0}"'.format(os.path.dirname(phutil_path)))
        local.run('git clone git://github.com/facebook/libphutil.git "{0}"'
                .format(phutil_path))

    local.cd(arc_path)
    local.run('git pull')
    local.cd(phutil_path)
    local.run('git pull')

def get_clean_hive():
    # Gets latest Hive from Apache Git repository and cleans the repository
    # (undo of any changes and removal of all generated files).  Also runs
    # `arc-setup` so the repo is ready to be used.
    print('\n-- Updating Hive repo\n')

    if local.run('test -d "{0}"'.format(code_path), warn_only = True,
            abandon_output = False) is None:
      local.run('mkdir -p "{0}"'.format(os.path.dirname(code_path)))
      local.run('git clone git://git.apache.org/hive.git "{0}"'.format(code_path))

    local.cd(code_path)
    local.run('git reset --hard HEAD')
    local.run('git clean -dffx')
    local.run('git pull')
    local.run('ant arc-setup')

def copy_local_hive():
    # Copy local repo to the destination path instead of using git clone
    if local.run('test -d "{0}"'.format(code_path), warn_only = True,
            abandon_output = False) is None:
      local.run('mkdir -p "{0}"'.format(os.path.dirname(code_path)))
    local.run('rm -rf "{0}"'.format(code_path), warn_only = True)
    local.run('mkdir -p "{0}"'.format(code_path))
    local.run('echo "{0}"'.format(runtest_dir))
    local.cd(runtest_dir)
    local.run('cp -rf * "{0}"'.format(code_path))
    local.cd(code_path)
    local.run('ant arc-setup')

def prepare_for_reports():
    # Generates directories for test reports.  All test nodes will copy results
    # to this directories.
    print('\n-- Creating a directory for JUnit reports\n')
    # Remove previous reports that might be there.
    local.run('rm -rf "{0}"'.format(report_path), warn_only = True)
    local.run('mkdir -p "{0}/logs"'.format(report_path))
    local.run('mkdir -p "{0}/out/clientpositive"'.format(report_path))
    local.run('mkdir -p "{0}/out/clientnegative"'.format(report_path))

def patch_hive(patches = [], revision = None):
    # Applies given patches to the Hive repo.  Revision means a Differential
    # revision, patches list is a list of paths to patches on local file system.
    #
    # Allowing multiple revisions and patches would complicate things a little
    # (order of applied patches should be preserved, but argparse will split
    # them into two lists) so only multiple local patches are allowed.
    # Shouldn't be a big problem as you can use `arc export` to get the patches
    # locally.
    local.cd(code_path)
    if revision is not None:
        print('\n-- Patching Hive repo using a Differential revision\n')
        revision = revision.upper()
        if not revision.startswith('D'):
            revision = 'D' + revision
        local.run('arc patch "{0}"'.format(revision))
    if patches:
        print('\n-- Patching Hive repo using a patch from local file system\n')
        for patch in patches:
            local.run('patch -rf -p0 < "{0}"'.format(patch))

def build_hive():
    print('\n-- Building Hive\n')
    local.cd(code_path)
    local.run('ant clean package')

def propagate_hive():
    # Expects master_base_path to be available on all test nodes in the same
    # place (for example using NFS).
    print('\n-- Propagating Hive repo to all hosts\n')
    print(host_code_path)
    print(code_path)
    remote_set.run('rm -rf "{0}"'.format(host_code_path))
    remote_set.run('mkdir -p "{0}"'.format(host_code_path))
    remote_set.run('cp -r "{0}/*" "{1}"'.format(
                    code_path, host_code_path))


def segment_tests(path):
    # Removes `.q` files that should not be run on this host.  The huge shell
    # command is slow (not really suprising considering amount of forking it has
    # to do), you are welcome to make it better=).
    local.cd(code_path + path)
    tests = local.run('ls -1', quiet = True, abandon_output = False).strip().split('\n')

    qfile_set.cd(host_code_path + path)
    cmd = []
    i = 0
    for test in tests:
        host = qfile_set.conn[i].hostname
        cmd.append('if [[ "{host}" != "' + host + '" ]]; then rm -f "' + test + '"; fi')
        i = (i + 1) % len(qfile_set)
    cmd = ' && '.join(cmd)
    # The command is huge and printing it out is not very useful, using wabbit
    # hunting mode.
    qfile_set.run(cmd, vewy_quiet = True)

def prepare_tests():
    print('\n-- Preparing test sets on all hosts\n')
    segment_tests('/ql/src/test/queries/clientpositive')
    segment_tests('/ql/src/test/queries/clientnegative')

def collect_log(name):
    # Moves JUnit log to the global logs directory.
    #
    # This has the same restriction on master_base_path as propagate_hive.
    new_name = name.split('.')
    new_name[-2] += '-{host}'
    new_name = '.'.join(new_name)
    qfile_set.cd(host_code_path + '/build/ql/test')
    # If tests failed there may be no file, so warn only if `cp` is having
    # problems.
    qfile_set.run(
        'cp "' + name + '" "' + report_path + '/logs/' + new_name + '" || ' +
        'touch "' + report_path + '/logs/{host}-' + name + '.fail"'
    )
    # Get the hive.log too.
    qfile_set.cd(host_code_path + '/build/ql/tmp')
    qfile_set.run('cp "hive.log" "' + report_path + '/logs/hive-{host}-' + name + '.log"',
            warn_only = True)

def collect_out(name, desc_name):
    # Moves `.out` file (test output) to the global logs directory.
    #
    # This has the same restriction on master_base_path as propagate_hive.
    qfile_set.cd(host_code_path + '/' + name)
    # Warn only if no files are found.
    qfile_set.run('mkdir -p "' + report_path + '/' + desc_name + '/out/' + '"', warn_only = True)
    qfile_set.run('cp * "' + report_path + '/' + desc_name + '/out/' + '"', warn_only = True)

def run_tests():
    # Runs TestCliDriver and TestNegativeCliDriver testcases.
    print('\n-- Running .q file tests on all hosts\n')

    # Using `quiet` because output of `ant test` is not very useful when we are
    # running on many hosts and it all gets mixed up.  In case of an error
    # you'll get last lines generated by `ant test` anyway (might be totally
    # irrelevant if one of the first tests fails and Ant reports a failure after
    # running all the other test, fortunately JUnit report saves the Ant output
    # if you need it for some reason).

    qfile_set.cd(host_code_path)
    qfile_set.run('ant -Dtestcase=TestCliDriver test',
            quiet = True, warn_only = True)
    collect_log('TEST-org.apache.hadoop.hive.cli.TestCliDriver.xml')
    collect_out('build/ql/test/logs/clientpositive', 'TestCliDriver')

    qfile_set.cd(host_code_path)
    qfile_set.run('ant -Dtestcase=TestNegativeCliDriver test',
            quiet = True, warn_only = True)
    collect_log('TEST-org.apache.hadoop.hive.cli.TestNegativeCliDriver.xml')
    collect_out('build/ql/test/logs/clientnegative', 'TestNegativeCliDriver')

def run_other_tests():
    # Runs all other tests that run_test doesn't run.

    def get_other_list():
        local.cd(code_path)
        # Generate test classes in build.
        local.run('ant -Dtestcase=nothing test')

        if (args.singlehost):
          tests = local.run(' | '.join([
              'find build/*/test/classes -name "Test*.class"',
              'sed -e "s:[^/]*/::g"',
              'grep -v TestSerDe.class',
              'grep -v TestHiveMetaStore.class',
              'grep -v TestCliDriver.class',
              'grep -v TestNegativeCliDriver.class',
              'grep -v ".*\$.*\.class"',
              'sed -e "s:\.class::"'
          ]), abandon_output = False)
          return tests.split()
        else:
          tests = local.run(' | '.join([
              'find build/*/test/classes -name "Test*.class"',
              'sed -e "s:[^/]*/::g"',
              'grep -v TestSerDe.class',
              'grep -v TestHiveMetaStore.class',
              'grep -v TestCliDriver.class',
              'grep -v TestNegativeCliDriver.class',
              'grep -v ".*\$.*\.class"',
              'grep -v TestSetUGIOnBothClientServer.class',
              'grep -v TestSetUGIOnOnlyClient.class',
              'grep -v TestSetUGIOnOnlyServer.class',
              'grep -v TestRemoteHiveMetaStore',
              'grep -v TestEmbeddedHiveMetaStore',
              'sed -e "s:\.class::"'
          ]), abandon_output = False)
          return tests.split()

    def segment_other():
        other_set.run('mkdir -p ' + report_path + '/TestContribCliDriver', warn_only = True)
        other_set.run('mkdir -p ' + report_path + '/TestContribCliDriver/positive', warn_only = True)
        other_set.run('mkdir -p ' + report_path + '/TestContribCliDriver/negative', warn_only = True)
        other_set.run('mkdir -p ' + report_path + '/TestHBaseCliDriver', warn_only = True)

        # Split all test cases between hosts.
        def get_command(test):
            return '; '.join([
                'ant -Dtestcase=' + test + ' test',

                'cp "`find . -name "TEST-*.xml"`" "' + report_path + '/logs/" || ' +
                'touch "' + report_path + '/logs/{host}-' + test + '.fail"',

                'cp "build/ql/tmp/hive.log" "' + report_path + '/logs/hive-{host}-' + test + '.log"',

                'cp "build/contrib/test/logs/contribclientnegative/*" "' + report_path + '/TestContribCliDriver/negative 2>/dev/null"',

                'cp "build/contrib/test/logs/contribclientpositive/*" "' + report_path + '/TestContribCliDriver/positive 2>/dev/null"',

                'cp "build/hbase-handler/test/logs/hbase-handler/*" "' + report_path + '/TestHBaseCliDriver/ 2>/dev/null"'
            ])

        cmd = []
        i = 0
        for test in get_other_list():
            # Special case, don't run minimr tests in parallel.  They will run
            # on the first host, and no other tests will run there (unless we
            # have a single host).
            #
            # TODO: Real fix would be to allow parallel runs of minimr tests.
            if len(other_set) > 1:
                if re.match('.*minimr.*', test.lower()):
                    host = other_set.conn[0].hostname
                else:
                    i = (i + 1) % len(other_set)
                    if i == 0:
                        i = 1
                    host = other_set.conn[i].hostname
            else:
                # We are running on single host.
                host = other_set.conn[0].hostname
            cmd.append(
                'if [[ "{host}" == "' + host + '" ]]; then ' +
                get_command(test) +
                '; fi'
            )
        return ' ; '.join(cmd)

    command = segment_other()
    other_set.cd(host_code_path)
    # See comment about quiet option in run_tests.
    other_set.run(command, quiet = True, warn_only = True)

def generate_report(one_file_report = False):
    # Uses `Report.py` to create a HTML report.
    print('\n-- Generating a test report\n')
    local.run('cp "' + master_base_path + '/templogs/* " "'+ report_path + '/logs/" ', warn_only = True)

    # Call format to remove '{{' and '}}'.
    path = os.path.expandvars(report_path.format())
    CmdArgs = collections.namedtuple('CmdArgs', ['one_file', 'log_dir', 'report_dir'])
    args = CmdArgs(
        one_file = one_file_report,
        log_dir = '{0}/logs'.format(path),
        report_dir = path
    )
    Report.make_report(args)

    print('\n-- Test report has been generated and is available here:')
    print('-- "{0}/report.html"'.format(path))
    print()

def stop_tests():
    # Brutally stops tests on all hosts, something more subtle would be nice and
    # would allow the same user to run this script multiple times
    # simultaneously.
    print('\n-- Stopping tests on all hosts\n')
    remote_set.run('killall -9 java', warn_only = True)

def remove_code():
    # Running this only on one connection per host so there are no conflicts
    # between several `rm` calls.  This removes all repositories, it would have
    # to be changed if we were to allow multiple simultaneous runs of this
    # script.

    print('\n-- Removing Hive code from all hosts\n')
    # We could remove only `host_code_path`, but then we would have abandoned
    # directories after lowering number of processes running on one host.
    cmd = 'rm -rf "' + host_base_path + '"'
    cmd = 'if [[ `echo "{host}" | grep -q -- "-0$"; echo "$?"` -eq "0" ]]; then ' + \
            cmd + '; fi'
    remote_set.run(cmd)

def overwrite_results():
    # Copy generated `.q.out` files to master repo.

    local.cd(code_path)
    expanded_path = local.run('pwd', abandon_output = False)
    print('\n-- Copying generated `.q.out` files to master repository: ' +
            expanded_path)

    for name in ['clientpositive', 'clientnegative']:
        local.cd(report_path + '/out/' + name)
        # Don't panic if no files are found.
        local.run('cp * "' + code_path + '/ql/src/test/results/' + name + '"',
                warn_only = True)

# -- Tasks that can be called from command line start here.

def cmd_prepare(patches = [], revision = None):
    get_ant()
    get_arc()
    if (args.copylocal):
      copy_local_hive()
    else :
      get_clean_hive()
      patch_hive(patches, revision)

    build_hive()
    propagate_hive()
    prepare_tests()

def cmd_run_tests(one_file_report = False):
    t = Thread(target = run_other_tests)
    t.start()
    prepare_for_reports()
    run_tests()
    t.join()

    if args.overwrite:
        overwrite_results()

    generate_report(one_file_report)

def cmd_test(patches = [], revision = None, one_file_report = False):
    cmd_prepare(patches, revision)

    if args.singlehost==False:
      local.cd(master_base_path + '/trunk')
      local.run('chmod -R 777 *');
      local.run('rm -rf "' + master_base_path + '/templogs/"')
      local.run('mkdir -p "' + master_base_path + '/templogs/"')
      tests = ['TestRemoteHiveMetaStore','TestEmbeddedHiveMetaStore','TestSetUGIOnBothClientServer','TestSetUGIOnOnlyClient','TestSetUGIOnOnlyServer']
      for test in tests:
        local.run('sudo -u hadoop ant -Dtestcase=' + test + ' test')
        local.run('cp "`find . -name "TEST-*.xml"`" "' + master_base_path + '/templogs/"')

    cmd_run_tests(one_file_report)

def cmd_stop():
    stop_tests()

def cmd_remove():
    remove_code()

parser = argparse.ArgumentParser(description =
        'Hive test farm controller.')
parser.add_argument('--config', dest = 'config',
        help = 'Path to configuration file')
parser.add_argument('--prepare', action = 'store_true', dest = 'prepare',
        help = 'Builds Hive and propagates it to all test machines')
parser.add_argument('--run-tests', action = 'store_true', dest = 'run_tests',
        help = 'Runs tests on all test machines')
parser.add_argument('--test', action = 'store_true', dest = 'test',
        help = 'Same as running `prepare` and then `run-tests`')
parser.add_argument('--report-name', dest = 'report_name',
        help = 'Store report and logs directory called `REPORT_NAME`')
parser.add_argument('--stop', action = 'store_true', dest = 'stop',
        help = 'Kill misbehaving tests on all machines')
parser.add_argument('--remove', action = 'store_true', dest = 'remove',
        help = 'Remove Hive trunk copies from test machines')
parser.add_argument('--revision', dest = 'revision',
        help = 'Differential revision to test')
parser.add_argument('--patch', dest = 'patch', nargs = '*',
        help = 'Patches from local file system to test')
parser.add_argument('--one-file-report', dest = 'one_file_report',
        action = 'store_true',
        help = 'Generate one (huge) report file instead of multiple small ones')
parser.add_argument('--overwrite', dest = 'overwrite', action = 'store_true',
        help = 'Overwrite result files in master repo')
parser.add_argument('--copylocal', dest = 'copylocal', action = 'store_true',
        help = 'Copy local repo instead of using git clone and git hub')
parser.add_argument('--singlehost', dest = 'singlehost', action = 'store_true',
        help = 'Only run the test on single host, It is the users '
               'responsibility to make sure that the conf. file does not '
               'contain multiple hosts. '
               'The script is not doing any validation. When --singlehost is set '
               'the script should not be run using sudo.')

args = parser.parse_args()

read_conf(args.config)

if args.report_name:
    report_path = '/'.join(report_path.split('/')[:-1] + [args.report_name])

if args.prepare:
    cmd_prepare(args.patch, args.revision)
elif args.run_tests:
    cmd_run_tests(args.one_file_report)
elif args.test:
    cmd_test(args.patch, args.revision, args.one_file_report)
elif args.stop:
    cmd_stop()
elif args.remove:
    cmd_remove()
