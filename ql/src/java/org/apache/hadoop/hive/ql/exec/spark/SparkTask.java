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

package org.apache.hadoop.hive.ql.exec.spark;

import java.util.List;

import org.apache.hadoop.hive.ql.DriverContext;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.plan.SparkWork;
import org.apache.hadoop.hive.ql.plan.api.StageType;
import org.apache.hadoop.util.StringUtils;

public class SparkTask extends Task<SparkWork> {
  private static final long serialVersionUID = 1L;

  @Override
  public int execute(DriverContext driverContext) {
    int rc = 1;
    SparkClient client = null;
    try {
      client = SparkClient.getInstance(driverContext.getCtx().getConf());
      rc = client.execute(driverContext, getWork());
    } finally {
      if (client != null) {
        rc = close(rc);
      }
    }
    return rc;
  }

  /**
   * close will move the temp files into the right place for the fetch
   * task. If the job has failed it will clean up the files.
   */
  private int close(int rc) {
    try {
      List<BaseWork> ws = work.getAllWork();
      for (BaseWork w: ws) {
        for (Operator<?> op: w.getAllOperators()) {
          op.jobClose(conf, rc == 0);
        }
      }
    } catch (Exception e) {
      // jobClose needs to execute successfully otherwise fail task
      if (rc == 0) {
        rc = 3;
        String mesg = "Job Commit failed with exception '"
            + Utilities.getNameMessage(e) + "'";
        console.printError(mesg, "\n" + StringUtils.stringifyException(e));
      }
    }
    return rc;
  }

  @Override
  public boolean isMapRedTask() {
    return true;
  }

  @Override
  public StageType getType() {
    return StageType.MAPRED;
  }

  @Override
  public String getName() {
    return "SPARK";
  }

}
