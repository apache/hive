#!/usr/local/bin/thrift -java

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

#
# Thrift Service that the hive service is built on
#

#
# TODO: include/thrift is shared among different components. It
# should not be under metastore.

include "share/fb303/if/fb303.thrift"
include "metastore/if/hive_metastore.thrift"
include "ql/if/queryplan.thrift"

namespace java org.apache.hadoop.hive.service
namespace cpp Apache.Hadoop.Hive

// Enumeration of JobTracker.State                                                                      
enum JobTrackerState {                                                                   
  INITIALIZING   = 1,           
  RUNNING        = 2,                                                      
}  

// Map-Reduce cluster status information
struct HiveClusterStatus {
  1: i32              taskTrackers,
  2: i32              mapTasks,
  3: i32              reduceTasks,
  4: i32              maxMapTasks,
  5: i32              maxReduceTasks,
  6: JobTrackerState  state,
}

exception HiveServerException {
  1: string message
  2: i32 errorCode
  3: string SQLState
}

# Interface for Thrift Hive Server
service ThriftHive extends hive_metastore.ThriftHiveMetastore {
  # Execute a query. Takes a HiveQL string
  void execute(1:string query) throws(1:HiveServerException ex)

  # Fetch one row. This row is the serialized form
  # of the result of the query
  string fetchOne() throws(1:HiveServerException ex)

  # Fetch a given number of rows or remaining number of
  # rows whichever is smaller.
  list<string> fetchN(1:i32 numRows) throws(1:HiveServerException ex)

  # Fetch all rows of the query result
  list<string> fetchAll() throws(1:HiveServerException ex)

  # Get a schema object with fields represented with native Hive types
  hive_metastore.Schema getSchema() throws(1:HiveServerException ex)

  # Get a schema object with fields represented with Thrift DDL types
  hive_metastore.Schema getThriftSchema() throws(1:HiveServerException ex)
  
  # Get the status information about the Map-Reduce cluster
  HiveClusterStatus getClusterStatus() throws(1:HiveServerException ex)
  
  # Get the queryplan annotated with counter information
  queryplan.QueryPlan getQueryPlan() throws(1:HiveServerException ex)

  # clean up last Hive query (releasing locks etc.)
  void clean()
}
