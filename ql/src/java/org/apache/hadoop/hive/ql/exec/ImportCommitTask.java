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

package org.apache.hadoop.hive.ql.exec;

import org.apache.hadoop.hive.ql.DriverContext;
import org.apache.hadoop.hive.ql.parse.ExplainConfiguration.AnalyzeState;
import org.apache.hadoop.hive.ql.plan.api.StageType;
import org.apache.hadoop.util.StringUtils;

public class ImportCommitTask extends Task<ImportCommitWork> {

  private static final long serialVersionUID = 1L;

  public ImportCommitTask() {
    super();
  }

  @Override
  public int execute(DriverContext driverContext) {
    if (Utilities.FILE_OP_LOGGER.isTraceEnabled()) {
      Utilities.FILE_OP_LOGGER.trace("Executing ImportCommit for " + work.getTxnId());
    }

    try {
      if (driverContext.getCtx().getExplainAnalyze() == AnalyzeState.RUNNING) {
        return 0;
      }
      return 0;
    } catch (Exception e) {
      console.printError("Failed with exception " + e.getMessage(), "\n"
          + StringUtils.stringifyException(e));
      setException(e);
      return 1;
    }
  }

  @Override
  public StageType getType() {
    return StageType.MOVE; // The commit for import is normally done as part of MoveTask.
  }

  @Override
  public String getName() {
    return "IMPORT_COMMIT";
  }
}
