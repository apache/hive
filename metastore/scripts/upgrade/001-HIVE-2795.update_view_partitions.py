#!/usr/local/bin/python
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.
#

# This script, provided with a list of view partitions, drop each partition and
# add them back via the metastore Thrift server.  This is needed because prior
# to HIVE-2795 view partitions were created without storage descriptors, which
# breaks commands such as DESCRIBE FORMATTED when called on these partitions.
# Altering a view's partition is not currently supported via the Hive CLI, and
# it results in an exception when attempted through the metastore Thrift server
# (due to the storage descriptor being null) so no data will be lost by dropping
# and adding the partition.
#
# WARNING: dropping and adding the partition is non-atomic.  The script outputs
#          each line of the file as it processes it.  You should pipe this
#          ouptut to a log file so that, if the machine fails between dropping
#          and adding, you know which partition may not have been added.  If it
#          has not, go to the Hive CLI and run the command
#
#          ALTER VIEW <view_name> ADD PARTITION (<part_spec>);
#
#          where view_name is the name of the view, which can be taken directly
#          from the line in the log, and part_spec is the partition
#          specification, which can be determined from the line in the log
#          E.g. if the partition name is col1=a/col2=b/col3=c part_spec should
#               be col1='a', col2='b', col3='c'
#
# NOTE: If any partition contains characters which are escaped, this script will
#       not work, this includes ASCII values 1-31,127 and the characters
#       " # % ' * / : = ? \ { [ ]

# Before running this script first execute the following query against your
# metastore:
#
# SELECT name, tbl_name, part_name
# FROM
#   DBS d JOIN TBLS t ON d.db_id = t.db_id
#   JOIN PARTITIONS p ON t.tbl_id = p.tbl_id
#   WHERE t.tbl_type = "VIRTUAL_VIEW";
#
# Place the results of this query in a file.  The format of the file should be
# as follow:
#
# db_name<sep>tbl_name<sep>part_name
#
# where <sep> represents a column separator (tab by default).
#
# Then execute this script passing in the path to the file you created, as well
# as the metastore host, port, and timeout and the separator used in the file if
# they differ from the defaults.

# To run this script you need Thrift Python library, as well as Hive's metastore
# Python library in your PYTHONPATH, Hive's metastore Python library can be
# found in trunk/build/dist/lib/py/

from optparse import OptionGroup
from optparse import OptionParser

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

from hive_metastore import ThriftHiveMetastore

# Parse args
parser = OptionParser()

mandatory_options = OptionGroup(parser, "Mandatory Settings",
                          "These must be set, they have no defaults")

mandatory_options.add_option("--file", action="store", type="string", dest="file",
                          help="file containing the list of view partitions " +
                               "stored as db_name<sep>table_name<sep>part_name")

parser.add_option_group(mandatory_options)

other_options = OptionGroup(parser, "Other Options",
                            "These options all have default values")

other_options.add_option("--host", action="store", type="string", dest="host",
                          default="localhost",
                          help="hostname of metastore server, " +
                               "the default is localhost")
other_options.add_option("--port", action="store", type="string", dest="port",
                          default="9083",
                          help="port for metastore server, the default is 9083")
other_options.add_option("--timeout", action="store", type="string", dest="timeout",
                          default=None,
                          help="timeout for connection to metastore server, " +
                               "uses Thrift's default")
other_options.add_option("--separator", action="store", type="string", dest="separator",
                          default="\t",
                          help="the separator between db_name, table_name, and " +
                               "part_name in the file passed in, the default " +
                               "is tab")

parser.add_option_group(other_options)

(options, args) = parser.parse_args()

host = options.host
port = options.port
timeout = options.timeout
file = options.file
separator = options.separator

# Prepare the Thrift connection to the metastore

_socket = TSocket.TSocket(host, port)
_socket.setTimeout(timeout)
_transport = TTransport.TBufferedTransport(_socket)
_protocol = TBinaryProtocol.TBinaryProtocol(_transport)

client = ThriftHiveMetastore.Client(_protocol)
_transport.open()

# Iterate over the file of partitions

partition_file=open(file,'r')
db_name = ''
table_name = ''
part_name = ''

for line in partition_file:

    line = line.rstrip("\n\r")
    (db_name,table_name,part_name)=line.split(separator)

    print line

    # Get the partition associated with this line

    partition = client.get_partition_by_name(db_name, table_name, part_name)

    # Drop it

    client.drop_partition_by_name(db_name, table_name, part_name, 0)

    # Add it back

    client.add_partition(partition)
