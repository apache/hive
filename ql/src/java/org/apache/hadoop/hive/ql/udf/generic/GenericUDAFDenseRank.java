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

import static org.apache.hadoop.hive.ql.util.DirectionUtils.ASCENDING_CODE;
import static org.apache.hadoop.hive.ql.util.DirectionUtils.DESCENDING_CODE;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.
        writableLongObjectInspector;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.WindowFunctionDescription;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.LongWritable;

@Description(
        name = "dense_rank",
        value = "_FUNC_(x) The difference between RANK and DENSE_RANK is that DENSE_RANK leaves no " +
                "gaps in ranking sequence when there are ties. That is, if you were " +
                "ranking a competition using DENSE_RANK and had three people tie for " +
                "second place, you would say that all three were in second place and " +
                "that the next person came in third.")
@WindowFunctionDescription(
        supportsWindow = false,
        pivotResult = true,
        rankingFunction = true,
        orderedAggregate = true)
public class GenericUDAFDenseRank extends GenericUDAFRank {

  @Override
  protected GenericUDAFAbstractRankEvaluator createWindowingEvaluator() {
    return new GenericUDAFDenseRankEvaluator();
  }

  @Override
  protected GenericUDAFHypotheticalSetRankEvaluator createHypotheticalSetEvaluator() {
    return new GenericUDAFHypotheticalSetDenseRankEvaluator();
  }

  public static class GenericUDAFDenseRankEvaluator extends GenericUDAFRankEvaluator {

    /*
     * Called when the value in the partition has changed. Update the currentRank
     */
    @Override
    protected void nextRank(RankBuffer rb) {
      rb.currentRank++;
    }
  }

  /**
   * Evaluator for calculating the dense rank.
   * SELECT dense_rank(expression1[, expressionn]*) WITHIN GROUP (ORDER BY col1[, coln]*)
   * Implementation is based on hypothetical rank calculation but the group of values are considered distinct.
   * Since the source of the input stream is not sorted a HashSet is used for filter out duplicate values
   * which can lead to OOM in large data sets.
   */
  public static class GenericUDAFHypotheticalSetDenseRankEvaluator extends GenericUDAFHypotheticalSetRankEvaluator {

    public GenericUDAFHypotheticalSetDenseRankEvaluator() {
      super(false, writableLongObjectInspector, writableLongObjectInspector);
    }

    @Override
    protected void initPartial2AndFinalOI(ObjectInspector[] parameters) {
      // nop
    }

    private static final class RowData {
      private final List<Object> columnValues;

      private RowData(List<Object> columnValues) {
        this.columnValues = columnValues;
      }

      @Override
      public boolean equals(Object o) {
        if (this == o) {
          return true;
        }
        if (o == null || getClass() != o.getClass()) {
          return false;
        }
        RowData rowData = (RowData) o;
        return Objects.equals(columnValues, rowData.columnValues);
      }

      @Override
      public int hashCode() {
        return Objects.hash(columnValues);
      }
    }

    private static class HypotheticalSetDenseRankBuffer extends AbstractAggregationBuffer {
      protected Set<RowData> elements = new HashSet<>();
      private long rank = 0;
    }

    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
      return new HypotheticalSetDenseRankBuffer();
    }

    @Override
    public void reset(AggregationBuffer agg) throws HiveException {
      HypotheticalSetDenseRankBuffer rankBuffer = (HypotheticalSetDenseRankBuffer) agg;
      rankBuffer.elements.clear();
      rankBuffer.rank = 0;
    }

    @Override
    public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
      HypotheticalSetDenseRankBuffer rankBuffer = (HypotheticalSetDenseRankBuffer) agg;

      CompareResult compareResult = compare(parameters);
      if (compareResult.getCompareResult() == 0) {
        return;
      }

      if (compareResult.getOrder() == ASCENDING_CODE && compareResult.getCompareResult() < 0 ||
              compareResult.getOrder() == DESCENDING_CODE && compareResult.getCompareResult() > 0) {
        List<Object> columnValues = new ArrayList<>(parameters.length / 4);
        for (int i = 0; i < parameters.length / 4; ++i) {
          columnValues.add(parameters[i * 4 + 1]);
        }
        RowData rowData = new RowData(columnValues);
        if (!rankBuffer.elements.contains(rowData)) {
          rankBuffer.elements.add(rowData);
          rankBuffer.rank++;
        }
      }
    }

    @Override
    public Object terminatePartial(AggregationBuffer agg) throws HiveException {
      HypotheticalSetDenseRankBuffer rankBuffer = (HypotheticalSetDenseRankBuffer) agg;
      return new LongWritable(rankBuffer.rank + 1);
    }


    @Override
    public void merge(AggregationBuffer agg, Object partial) throws HiveException {
      if (partial == null) {
        return;
      }

      HypotheticalSetDenseRankBuffer rankBuffer = (HypotheticalSetDenseRankBuffer) agg;
      rankBuffer.rank += ((LongWritable)partial).get() - 1;
    }

    @Override
    public Object terminate(AggregationBuffer agg) throws HiveException {
      HypotheticalSetDenseRankBuffer rankBuffer = (HypotheticalSetDenseRankBuffer) agg;
      return new LongWritable(rankBuffer.rank + 1);
    }
  }
}
