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

package org.apache.hadoop.hive.ql.exec.repl;

import org.apache.hadoop.hive.ql.DriverContext;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.plan.api.StageType;

import java.io.Serializable;

/**
 * ReplStateLogTask.
 *
 * Exists for the sole purpose of reducing the number of dependency edges in the task graph.
 **/
public class ReplStateLogTask extends Task<ReplStateLogWork> implements Serializable {

  private static final long serialVersionUID = 1L;

  @Override
  public int execute(DriverContext driverContext) {
    work.replStateLog();
    return 0;
  }

  @Override
  public StageType getType() {
    return StageType.REPL_STATE_LOG;
  }

  @Override
  public String getName() {
    return "REPL_STATE_LOG";
  }
}
