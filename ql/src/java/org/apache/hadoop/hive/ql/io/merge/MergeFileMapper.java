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

package org.apache.hadoop.hive.ql.io.merge;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.ql.exec.AbstractFileMergeOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

/**
 * Mapper for fast file merging of ORC and RC files. This is very similar to
 * ExecMapper except that root operator is AbstractFileMergeOperator. This class
 * name is used for serialization and deserialization of MergeFileWork.
 */
public class MergeFileMapper extends MapReduceBase implements Mapper {
  public static final Logger LOG = LoggerFactory.getLogger("MergeFileMapper");
  private static final String PLAN_KEY = "__MAP_PLAN__";

  private JobConf jc;
  private Operator<? extends OperatorDesc> op;
  private AbstractFileMergeOperator mergeOp;
  private Object[] row;
  private boolean abort;

  @Override
  public void configure(JobConf job) {
    jc = job;
    MapWork mapWork = Utilities.getMapWork(job);

    try {
      if (mapWork instanceof MergeFileWork) {
        MergeFileWork mfWork = (MergeFileWork) mapWork;
        String alias = mfWork.getAliasToWork().keySet().iterator().next();
        op = mfWork.getAliasToWork().get(alias);
        if (op instanceof AbstractFileMergeOperator) {
          mergeOp = (AbstractFileMergeOperator) op;
          mergeOp.initializeOp(jc);
          row = new Object[2];
          abort = false;
        } else {
          abort = true;
          throw new RuntimeException(
              "Merge file work's top operator should be an" +
                  " instance of AbstractFileMergeOperator");
        }
      } else {
        abort = true;
        throw new RuntimeException("Map work should be a merge file work.");
      }
    } catch (HiveException e) {
      abort = true;
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() throws IOException {
    try {
      mergeOp.closeOp(abort);
    } catch (HiveException e) {
      throw new IOException(e);
    }
    super.close();
  }

  @Override
  public void map(Object key, Object value, OutputCollector output,
      Reporter reporter) throws IOException {

    row[0] = key;
    row[1] = value;
    try {
      mergeOp.process(row, 0);
    } catch (HiveException e) {
      abort = true;
      throw new IOException(e);
    }
  }
}
