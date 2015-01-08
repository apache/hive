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

import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.AbstractFileMergeOperator;
import org.apache.hadoop.hive.ql.exec.ObjectCache;
import org.apache.hadoop.hive.ql.exec.ObjectCacheFactory;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.io.merge.MergeFileWork;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.FileMergeDesc;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import com.google.common.base.Preconditions;

/**
 * Copied from MergeFileMapper.
 *
 * As MergeFileMapper is very similar to ExecMapper, this class is
 * very similar to SparkMapRecordHandler
 */
public class SparkMergeFileRecordHandler extends SparkRecordHandler {

  private static final String PLAN_KEY = "__MAP_PLAN__";
  private static final Log LOG = LogFactory.getLog(SparkMergeFileRecordHandler.class);
  private Operator<? extends OperatorDesc> op;
  private AbstractFileMergeOperator<? extends FileMergeDesc> mergeOp;
  private Object[] row;

  @SuppressWarnings("unchecked")
  @Override
  public <K, V> void init(JobConf job, OutputCollector<K, V> output, Reporter reporter) throws Exception {
    super.init(job, output, reporter);

    ObjectCache cache = ObjectCacheFactory.getCache(job);

    try {
      jc = job;
      MapWork mapWork = (MapWork) cache.retrieve(PLAN_KEY);

      if (mapWork == null) {
        mapWork = Utilities.getMapWork(job);
        cache.cache(PLAN_KEY, mapWork);
      } else {
        Utilities.setMapWork(job, mapWork);
      }

      if (mapWork instanceof MergeFileWork) {
        MergeFileWork mergeFileWork = (MergeFileWork) mapWork;
        String alias = mergeFileWork.getAliasToWork().keySet().iterator().next();
        op = mergeFileWork.getAliasToWork().get(alias);
        if (op instanceof AbstractFileMergeOperator) {
          mergeOp = (AbstractFileMergeOperator<? extends FileMergeDesc>) op;
          mergeOp.initializeOp(jc);
          row = new Object[2];
          abort = false;
        } else {
          abort = true;
          throw new IllegalStateException(
              "Merge file work's top operator should be an"
                + " instance of AbstractFileMergeOperator");
        }
      } else {
        abort = true;
        throw new IllegalStateException("Map work should be a merge file work.");
      }

      LOG.info(mergeOp.dump(0));
    } catch (HiveException e) {
      abort = true;
      throw new RuntimeException(e);
    }
  }

  @Override
  public void processRow(Object key, Object value) throws IOException {
    row[0] = key;
    row[1] = value;
    try {
      mergeOp.processOp(row, 0);
    } catch (HiveException e) {
      abort = true;
      throw new IOException(e);
    }
  }

  @Override
  public <E> void processRow(Object key, Iterator<E> values) throws IOException {
    throw new UnsupportedOperationException("Do not support this method in "
        + this.getClass().getSimpleName());
  }

  @Override
  public void close() {
    LOG.info("Closing Merge Operator " + mergeOp.getName());
    try {
      mergeOp.closeOp(abort);
    } catch (HiveException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean getDone() {
    return mergeOp.getDone();
  }
}
