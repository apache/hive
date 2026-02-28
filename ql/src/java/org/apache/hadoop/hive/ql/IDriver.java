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

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hive.common.classification.InterfaceAudience;
import org.apache.hadoop.hive.common.classification.InterfaceStability;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Schema;
import org.apache.hadoop.hive.ql.exec.FetchTask;
import org.apache.hadoop.hive.ql.processors.CommandProcessor;
import org.apache.hadoop.hive.ql.processors.CommandProcessorException;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;

/**
 * Hive query executor driver.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public interface IDriver extends CommandProcessor {

  CommandProcessorResponse compileAndRespond(String statement) throws CommandProcessorException;

  QueryPlan getPlan();

  QueryState getQueryState();

  QueryDisplay getQueryDisplay();

  void setOperationId(String operationId);

  CommandProcessorResponse run() throws CommandProcessorException;

  @Override
  CommandProcessorResponse run(String command) throws CommandProcessorException;

  // create some "cover" to the result?
  @SuppressWarnings("rawtypes")
  boolean getResults(List res) throws IOException;

  void setMaxRows(int maxRows);

  FetchTask getFetchTask();

  Schema getSchema();

  boolean isFetchingTable();

  void resetFetch() throws IOException;

  // close&destroy is used in seq coupling most of the time - the difference is either not clear; or not relevant
  // remove?
  @Override
  void close();
  void destroy();

  HiveConf getConf();

  Context getContext();

  boolean hasResultSet();

}
