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

package org.apache.hadoop.hive.ql.udf.generic;

import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.
        writableDoubleObjectInspector;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.WindowFunctionDescription;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.io.IntWritable;

@Description(
        name = "cume_dist",
        value = "_FUNC_(x) - The CUME_DIST function (defined as the inverse of percentile in some " +
                "statistical books) computes the position of a specified value relative to a set of values. " +
                "To compute the CUME_DIST of a value x in a set S of size N, you use the formula: " +
                "CUME_DIST(x) =  number of values in S coming before " +
                "   and including x in the specified order/ N")
@WindowFunctionDescription(
        supportsWindow = false,
        pivotResult = true,
        rankingFunction = true,
        orderedAggregate = true)
public class GenericUDAFCumeDist extends GenericUDAFRank {

  @Override
  protected GenericUDAFAbstractRankEvaluator createWindowingEvaluator() {
    return new GenericUDAFCumeDistEvaluator();
  }

  @Override
  protected GenericUDAFHypotheticalSetRankEvaluator createHypotheticalSetEvaluator() {
    return new GenericUDAFHypotheticalSetCumeDistEvaluator();
  }

  public static class GenericUDAFCumeDistEvaluator extends GenericUDAFAbstractRankEvaluator {
    @Override
    public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
      super.init(m, parameters);
      return ObjectInspectorFactory
          .getStandardListObjectInspector(writableDoubleObjectInspector);
    }

    @Override
    public Object terminate(AggregationBuffer agg) throws HiveException {
      List<IntWritable> ranks = ((RankBuffer) agg).rowNums;
      int ranksSize = ranks.size();
      double ranksSizeDouble = ranksSize;
      List<DoubleWritable> distances = new ArrayList<DoubleWritable>(ranksSize);
      int last = -1;
      int current = -1;
      // tracks the number of elements with the same rank at the current time
      int elementsAtRank = 1;
      for (int index = 0; index < ranksSize; index++) {
        current = ranks.get(index).get();
        if (index == 0) {
          last = current;
        } else if (last == current) {
          elementsAtRank++;
        } else {
          last = current;
          double distance = ((double) index) / ranksSizeDouble;
          while (elementsAtRank-- > 0) {
            distances.add(new DoubleWritable(distance));
          }
          elementsAtRank = 1;
        }
      }
      if (ranksSize > 0 && last == current) {
        double distance = ((double) ranksSize) / ranksSizeDouble;
        while (elementsAtRank-- > 0) {
          distances.add(new DoubleWritable(distance));
        }
      }
      return distances;
    }
  }

  /**
   * Evaluator for calculating the cumulative distribution.
   * SELECT cume_dist(expression) WITHIN GROUP (ORDER BY col1)
   * Implementation is based on hypothetical rank calculation: (rank + 1) / (count + 1)
   * Differences:
   * - rows which has equal column value with the specified expression value should be counted in the rank
   * - the return value type of this function is double.
   */
  public static class GenericUDAFHypotheticalSetCumeDistEvaluator
          extends GenericUDAFHypotheticalSetRankEvaluator {

    public GenericUDAFHypotheticalSetCumeDistEvaluator() {
      super(true, PARTIAL_RANK_OI, writableDoubleObjectInspector);
    }

    @Override
    public Object terminate(AggregationBuffer agg) throws HiveException {
      HypotheticalSetRankBuffer rankBuffer = (HypotheticalSetRankBuffer) agg;
      return new DoubleWritable((rankBuffer.rank + 1.0) / (rankBuffer.rowCount + 1.0));
    }
  }
}
