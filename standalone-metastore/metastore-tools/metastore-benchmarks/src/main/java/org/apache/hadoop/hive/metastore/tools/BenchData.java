/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.metastore.tools;

/**
 * Container for all data needed for running a benchmark.
 */
final class BenchData {
  private HMSClient client;
  final String dbName;
  final String tableName;

  BenchData(String dbName, String tableName) {
    this.dbName = dbName;
    this.tableName = tableName;
  }

  HMSClient getClient() {
    return client;
  }

  void setClient(HMSClient client) {
    this.client = client;
  }
}

