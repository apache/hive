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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.repl.dump.Utils;
import org.apache.hadoop.hive.ql.plan.api.StageType;

import java.io.Serializable;

/**
 * AckTask.
 *
 * Add the repl dump/ repl load complete acknowledgement.
 **/
public class AckTask extends Task<AckWork> implements Serializable {

  private static final long serialVersionUID = 1L;

  @Override
  public int execute() {
    try {
      Path ackPath = work.getAckFilePath();
      Utils.create(ackPath, conf);
    } catch (SemanticException e) {
      setException(e);
      return ErrorMsg.getErrorMsg(e.getMessage()).getErrorCode();
    }
    return 0;
  }

  @Override
  public StageType getType() {
    return StageType.ACK;
  }

  @Override
  public String getName() {
    return "ACK_TASK";
  }

  @Override
  public boolean canExecuteInParallel() {
    // ACK_TASK must be executed only when all its parents are done with execution.
    return false;
  }
}
