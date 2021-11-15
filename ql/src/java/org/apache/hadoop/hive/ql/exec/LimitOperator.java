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
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.exec.tez.LlapObjectCache;
import org.apache.hadoop.hive.ql.exec.tez.TezProcessor;
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

  protected transient ObjectCache runtimeCache;
  protected transient String limitKey;

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

    String queryId = HiveConf.getVar(getConfiguration(), HiveConf.ConfVars.HIVEQUERYID);
    this.runtimeCache = ObjectCacheFactory.getCache(getConfiguration(), queryId, false, true);

    // this can happen in HS2 while doing local fetch optimization, where LimitOperator is used
    if (runtimeCache == null) {
      if (!HiveConf.isLoadHiveServer2Config()) {
        throw new IllegalStateException(
            "Cannot get a query cache object while working outside of HS2, this is unexpected");
      }
      // in HS2, this is the only LimitOperator instance for a query, it's safe to fake an object
      // for further processing
      this.runtimeCache = new LlapObjectCache();
    }
    this.limitKey = getOperatorId() + "_record_count";

    AtomicInteger currentCountForAllTasks = getCurrentCount();
    int currentCountForAllTasksInt = currentCountForAllTasks.get();

    if (currentCountForAllTasksInt >= limit) {
      LOG.info("LimitOperator exits early as query limit already reached: {} >= {}",
          currentCountForAllTasksInt, limit);
      onLimitReached();
    }
  }

  @Override
  public void process(Object row, int tag) throws HiveException {
    AtomicInteger currentCountForAllTasks = getCurrentCount();
    int currentCountForAllTasksInt = currentCountForAllTasks.get();

    if (offset <= currCount && currCount < (offset + limit) && offset <= currentCountForAllTasksInt
        && currentCountForAllTasksInt < (offset + limit)) {
      forward(row, inputObjInspectors[tag]);
      currCount++;
      currentCountForAllTasks.incrementAndGet();
    } else if (offset > currCount) {
      currCount++;
      currentCountForAllTasks.incrementAndGet();
    } else {
      onLimitReached();
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

  protected void onLimitReached() {
    super.setDone(true);

    String limitReachedKey = getLimitReachedKey(getConfiguration());

    try {
      runtimeCache.retrieve(limitReachedKey, new Callable<AtomicBoolean>() {
        @Override
        public AtomicBoolean call() {
          return new AtomicBoolean(false);
        }
      }).set(true);
    } catch (HiveException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void closeOp(boolean abort) throws HiveException {
    if (!isMap && currCount < leastRow) {
      throw new HiveException("No sufficient row found");
    }
    super.closeOp(abort);
  }

  public AtomicInteger getCurrentCount() {
    try {
      return runtimeCache.retrieve(limitKey, new Callable<AtomicInteger>() {
        @Override
        public AtomicInteger call() {
          return new AtomicInteger();
        }
      });
    } catch (HiveException e) {
      throw new RuntimeException(e);
    }
  }

  public static String getLimitReachedKey(Configuration conf) {
    return conf.get(TezProcessor.HIVE_TEZ_VERTEX_NAME) + "_limit_reached";
  }
}
