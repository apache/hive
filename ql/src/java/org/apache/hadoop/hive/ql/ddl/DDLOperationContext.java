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

package org.apache.hadoop.hive.ql.ddl;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;

/**
 * Context for DDL operations.
 */
public class DDLOperationContext {
  private final HiveConf conf;
  private final Context context;
  private final DDLTask task;
  private final DDLWork work;
  private final QueryState queryState;
  private final QueryPlan queryPlan;
  private final LogHelper console;

  public DDLOperationContext(HiveConf conf, Context context, DDLTask task, DDLWork work, QueryState queryState,
      QueryPlan queryPlan, LogHelper console){
    this.conf = conf;
    this.context = context;
    this.task = task;
    this.work = work;
    this.queryState = queryState;
    this.queryPlan = queryPlan;
    this.console = console;
  }

  public Hive getDb() throws HiveException {
    return Hive.get(conf);
  }

  public HiveConf getConf() {
    return conf;
  }

  public Context getContext() {
    return context;
  }

  public DDLTask getTask() {
    return task;
  }

  public DDLWork getWork() {
    return work;
  }

  public QueryState getQueryState() {
    return queryState;
  }

  public QueryPlan getQueryPlan() {
    return queryPlan;
  }

  public LogHelper getConsole() {
    return console;
  }
}
