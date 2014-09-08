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

package org.apache.hadoop.hive.ql.udf.generic;

import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.WindowFunctionDescription;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.IntWritable;

@WindowFunctionDescription(
  description = @Description(
    name = "percent_rank",
    value = "_FUNC_(x) PERCENT_RANK is similar to CUME_DIST, but it uses rank values rather " +
            "than row counts in its numerator. PERCENT_RANK of a row is calculated as: " +
            "(rank of row in its partition - 1) / (number of rows in the partition - 1)"
  ),
  supportsWindow = false,
  pivotResult = true,
  rankingFunction = true,
  impliesOrder = true
)
public class GenericUDAFPercentRank extends GenericUDAFRank {

  static final Log LOG = LogFactory.getLog(GenericUDAFPercentRank.class.getName());

  @Override
  protected GenericUDAFAbstractRankEvaluator createEvaluator() {
    return new GenericUDAFPercentRankEvaluator();
  }

  public static class GenericUDAFPercentRankEvaluator extends GenericUDAFAbstractRankEvaluator {

    @Override
    public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
      super.init(m, parameters);
      return ObjectInspectorFactory.getStandardListObjectInspector(
        PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);
    }

    @Override
    public Object terminate(AggregationBuffer agg) throws HiveException {
      ArrayList<IntWritable> ranks = ((RankBuffer) agg).rowNums;
      double sz = ranks.size();
      if (sz > 1) {
        sz = sz - 1;
      }
      ArrayList<DoubleWritable> pranks = new ArrayList<DoubleWritable>(ranks.size());

      for (IntWritable i : ranks) {
        double pr = ((double) i.get() - 1) / sz;
        pranks.add(new DoubleWritable(pr));
      }

      return pranks;
    }
  }
}

