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

package org.apache.hadoop.hive.ql;

import org.apache.hadoop.hive.conf.HiveConf;

/**
 * Constructs a driver for ql clients
 */
public class DriverFactory {

  enum ExecutionStrategy {
    none {
      @Override
      IDriver build(QueryState queryState, String userName, QueryInfo queryInfo) {
        return new Driver(queryState, userName, queryInfo);
      }
    };

    abstract IDriver build(QueryState queryState, String userName, QueryInfo queryInfo);
  }

  public static IDriver newDriver(HiveConf conf) {
    return newDriver(getNewQueryState(conf), null, null);
  }

  public static IDriver newDriver(QueryState queryState, String userName, QueryInfo queryInfo) {
    ExecutionStrategy strategy = ExecutionStrategy.none;
    return strategy.build(queryState, userName, queryInfo);
  }

  private static QueryState getNewQueryState(HiveConf conf) {
    return new QueryState.Builder().withGenerateNewQueryId(true).withHiveConf(conf).build();
  }
}
