/**
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

package org.apache.hadoop.hive.ql.optimizer.physical.index;

import java.io.Serializable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.parse.ParseContext;

public class IndexWhereProcCtx implements NodeProcessorCtx {

  private static final Logger LOG = LoggerFactory.getLogger(IndexWhereProcCtx.class.getName());

  private final Task<? extends Serializable> currentTask;
  private final ParseContext parseCtx;

  public IndexWhereProcCtx(Task<? extends Serializable> task, ParseContext parseCtx) {
    this.currentTask = task;
    this.parseCtx = parseCtx;
  }

  public ParseContext getParseContext() {
    return parseCtx;
  }

  public Task<? extends Serializable> getCurrentTask() {
    return currentTask;
  }
}
