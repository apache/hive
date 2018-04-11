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

import java.io.Serializable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.LimitDesc;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;

/**
 * Limit operator implementation Limits the number of rows to be passed on.
 **/
public class LimitOperator extends Operator<LimitDesc> implements Serializable {
  private static final long serialVersionUID = 1L;

  protected transient int limit;
  protected transient int offset;
  protected transient int leastRow;
  protected transient int currCount;
  protected transient boolean isMap;

  /** Kryo ctor. */
  protected LimitOperator() {
    super();
  }

  public LimitOperator(CompilationOpContext ctx) {
    super(ctx);
  }

  @Override
  protected void initializeOp(Configuration hconf) throws HiveException {
    super.initializeOp(hconf);
    limit = conf.getLimit();
    leastRow = conf.getLeastRows();
    offset = (conf.getOffset() == null) ? 0 : conf.getOffset();
    currCount = 0;
    isMap = hconf.getBoolean("mapred.task.is.map", true);
  }

  @Override
  public void process(Object row, int tag) throws HiveException {
    if (offset <= currCount && currCount < (offset + limit)) {
      forward(row, inputObjInspectors[tag]);
      currCount++;
    } else if (offset > currCount) {
      currCount++;
    } else {
      setDone(true);
    }
  }

  @Override
  public String getName() {
    return LimitOperator.getOperatorName();
  }

  static public String getOperatorName() {
    return "LIM";
  }

  @Override
  public OperatorType getType() {
    return OperatorType.LIMIT;
  }

  @Override
  public void closeOp(boolean abort) throws HiveException {
    if (!isMap && currCount < leastRow) {
      throw new HiveException("No sufficient row found");
    }
    super.closeOp(abort);
  }

}
