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
package org.apache.hadoop.hive.ql.udf.ptf;

import java.util.List;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.PTFOperator;
import org.apache.hadoop.hive.ql.exec.PTFPartition;
import org.apache.hadoop.hive.ql.exec.PTFPartition.PTFPartitionIterator;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.LeadLagInfo;
import org.apache.hadoop.hive.ql.parse.WindowingSpec.BoundarySpec;
import org.apache.hadoop.hive.ql.parse.WindowingSpec.Direction;
import org.apache.hadoop.hive.ql.parse.WindowingSpec.WindowType;
import org.apache.hadoop.hive.ql.plan.ptf.BoundaryDef;
import org.apache.hadoop.hive.ql.plan.ptf.PTFExpressionDef;
import org.apache.hadoop.hive.ql.plan.ptf.WindowFrameDef;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFAverage.AbstractGenericUDAFAverageEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AbstractAggregationBuffer;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AggregationBuffer;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFSum.GenericUDAFSumEvaluator;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

/**
 * This class is mostly used for RANGE windowing type to do some optimization. ROWS
 * windowing type can support streaming.
 *
 */
public class BasePartitionEvaluator {
  protected final GenericUDAFEvaluator wrappedEvaluator;
  protected final WindowFrameDef winFrame;
  protected final PTFPartition partition;
  protected final List<PTFExpressionDef> parameters;
  protected final ObjectInspector outputOI;

  /**
   * Internal class to represent a window range in a partition by searching the
   * relative position (ROWS) or relative value (RANGE) of the current row
   */
  protected static class Range
  {
    int start;
    int end;
    PTFPartition p;

    public Range(int start, int end, PTFPartition p)
    {
      this.start = start;
      this.end = end;
      this.p = p;
    }

    public PTFPartitionIterator<Object> iterator()
    {
      return p.range(start, end);
    }

    public int getDiff(Range prevRange) {
      return this.start - prevRange.start + this.end - prevRange.end;
    }

    public int getSize() {
      return end - start;
    }
  }

  /**
   * Define some type specific operation to used in the subclass
   */
  private static abstract class TypeOperationBase<ResultType> {
    public abstract ResultType add(ResultType t1, ResultType t2);
    public abstract ResultType minus(ResultType t1, ResultType t2);
    public abstract ResultType div(ResultType sum, long numRows);
  }

  private static class TypeOperationLongWritable extends TypeOperationBase<LongWritable> {
    @Override
    public LongWritable add(LongWritable t1, LongWritable t2) {
      if (t1 == null && t2 == null) return null;
      return new LongWritable((t1 == null ? 0 : t1.get()) + (t2 == null ? 0 : t2.get()));
    }

    @Override
    public LongWritable minus(LongWritable t1, LongWritable t2) {
      if (t1 == null && t2 == null) return null;
      return new LongWritable((t1 == null ? 0 : t1.get()) - (t2 == null ? 0 : t2.get()));
    }

    @Override
    public LongWritable div(LongWritable sum, long numRows) {
      return null; // Not used
    }
  }

  private static class TypeOperationDoubleWritable extends TypeOperationBase<DoubleWritable> {
    @Override
    public DoubleWritable add(DoubleWritable t1, DoubleWritable t2) {
      if (t1 == null && t2 == null) return null;
      return new DoubleWritable((t1 == null ? 0 : t1.get()) + (t2 == null ? 0 : t2.get()));
    }

    public DoubleWritable minus(DoubleWritable t1, DoubleWritable t2) {
      if (t1 == null && t2 == null) return null;
      return new DoubleWritable((t1 == null ? 0 : t1.get()) - (t2 == null ? 0 : t2.get()));
    }

    @Override
    public DoubleWritable div(DoubleWritable sum, long numRows) {
      if (sum == null || numRows == 0) return null;

      return new DoubleWritable(sum.get() / (double)numRows);
    }
  }

  private static class TypeOperationHiveDecimalWritable extends TypeOperationBase<HiveDecimalWritable> {
    @Override
    public HiveDecimalWritable div(HiveDecimalWritable sum, long numRows) {
      if (sum == null || numRows == 0) return null;

      HiveDecimalWritable result = new HiveDecimalWritable(sum);
      result.mutateDivide(HiveDecimal.create(numRows));
      return result;
    }

    @Override
    public HiveDecimalWritable add(HiveDecimalWritable t1, HiveDecimalWritable t2) {
      if (t1 == null && t2 == null) return null;

      if (t1 == null) {
        return new HiveDecimalWritable(t2);
      } else {
        HiveDecimalWritable result = new HiveDecimalWritable(t1);
        if (t2 != null) {
          result.mutateAdd(t2);
        }
        return result;
      }
    }

    @Override
    public HiveDecimalWritable minus(HiveDecimalWritable t1, HiveDecimalWritable t2) {
      if (t1 == null && t2 == null) return null;

      if (t2 == null) {
        return new HiveDecimalWritable(t1);
      } else {
        HiveDecimalWritable result = new HiveDecimalWritable(t2);
        result.mutateNegate();
        if (t1 != null) {
          result.mutateAdd(t1);
        }
        return result;
      }
    }
  }

  public BasePartitionEvaluator(
      GenericUDAFEvaluator wrappedEvaluator,
      WindowFrameDef winFrame,
      PTFPartition partition,
      List<PTFExpressionDef> parameters,
      ObjectInspector outputOI) {
    this.wrappedEvaluator = wrappedEvaluator;
    this.winFrame = winFrame;
    this.partition = partition;
    this.parameters = parameters;
    this.outputOI = outputOI;
  }

  /**
   * Get the aggregation for the whole partition. Used in the case where windowing
   * is unbounded or the function value is calculated based on all the rows in the
   * partition such as percent_rank().
   * @return the aggregated result
   * @throws HiveException
   */
  public Object getPartitionAgg() throws HiveException {
    return calcFunctionValue(partition.iterator(), null);
  }

  /**
   * Given the current row, get the aggregation for the window
   *
   * @throws HiveException
   */
  public Object iterate(int currentRow, LeadLagInfo leadLagInfo) throws HiveException {
    Range range = getRange(winFrame, currentRow, partition);
    PTFPartitionIterator<Object> pItr = range.iterator();
    return calcFunctionValue(pItr, leadLagInfo);
  }

  /**
   * Given a partition iterator, calculate the function value
   * @param pItr  the partition pointer
   * @return      the function value
   * @throws HiveException
   */
  protected Object calcFunctionValue(PTFPartitionIterator<Object> pItr, LeadLagInfo leadLagInfo)
      throws HiveException {
    // To handle the case like SUM(LAG(f)) over(), aggregation function includes
    // LAG/LEAD call
    PTFOperator.connectLeadLagFunctionsToPartition(leadLagInfo, pItr);

    AggregationBuffer aggBuffer = wrappedEvaluator.getNewAggregationBuffer();
    Object[] argValues = new Object[parameters == null ? 0 : parameters.size()];
    while(pItr.hasNext())
    {
      Object row = pItr.next();
      int i = 0;
      if ( parameters != null ) {
        for(PTFExpressionDef param : parameters)
        {
          argValues[i++] = param.getExprEvaluator().evaluate(row);
        }
      }
      wrappedEvaluator.aggregate(aggBuffer, argValues);
    }

    // The object is reused during evaluating, make a copy here
    return ObjectInspectorUtils.copyToStandardObject(wrappedEvaluator.evaluate(aggBuffer), outputOI);
  }

  protected static Range getRange(WindowFrameDef winFrame, int currRow, PTFPartition p)
      throws HiveException {
    BoundaryDef startB = winFrame.getStart();
    BoundaryDef endB = winFrame.getEnd();

    int start, end;
    if (winFrame.getWindowType() == WindowType.ROWS) {
      start = getRowBoundaryStart(startB, currRow);
      end = getRowBoundaryEnd(endB, currRow, p);
    } else {
      ValueBoundaryScanner vbs = ValueBoundaryScanner.getScanner(winFrame);
      start = vbs.computeStart(currRow, p);
      end = vbs.computeEnd(currRow, p);
    }
    start = start < 0 ? 0 : start;
    end = end > p.size() ? p.size() : end;
    return new Range(start, end, p);
  }

  private static int getRowBoundaryStart(BoundaryDef b, int currRow) throws HiveException {
    Direction d = b.getDirection();
    int amt = b.getAmt();
    switch(d) {
    case PRECEDING:
      if (amt == BoundarySpec.UNBOUNDED_AMOUNT) {
        return 0;
      }
      else {
        return currRow - amt;
      }
    case CURRENT:
      return currRow;
    case FOLLOWING:
      return currRow + amt;
    }
    throw new HiveException("Unknown Start Boundary Direction: " + d);
  }

  private static int getRowBoundaryEnd(BoundaryDef b, int currRow, PTFPartition p) throws HiveException {
    Direction d = b.getDirection();
    int amt = b.getAmt();
    switch(d) {
    case PRECEDING:
      if ( amt == 0 ) {
        return currRow + 1;
      }
      return currRow - amt + 1;
    case CURRENT:
      return currRow + 1;
    case FOLLOWING:
      if (amt == BoundarySpec.UNBOUNDED_AMOUNT) {
        return p.size();
      }
      else {
        return currRow + amt + 1;
      }
    }
    throw new HiveException("Unknown End Boundary Direction: " + d);
  }

  /**
   * The base type for sum operator evaluator when a partition data is available
   * and streaming process is not possible. Some optimization can be done for such
   * case.
   *
   */
  public static abstract class SumPartitionEvaluator<ResultType extends Writable> extends BasePartitionEvaluator {
    static class WindowSumAgg<ResultType> extends AbstractAggregationBuffer {
      Range prevRange;
      ResultType prevSum;
      boolean empty;
    }

    protected final WindowSumAgg<ResultType> sumAgg;
    protected TypeOperationBase<ResultType> typeOperation;

    public SumPartitionEvaluator(
        GenericUDAFEvaluator wrappedEvaluator,
        WindowFrameDef winFrame,
        PTFPartition partition,
        List<PTFExpressionDef> parameters,
        ObjectInspector outputOI) {
      super(wrappedEvaluator, winFrame, partition, parameters, outputOI);
      sumAgg = new WindowSumAgg<ResultType>();
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public Object iterate(int currentRow, LeadLagInfo leadLagInfo) throws HiveException {
      // Currently sum(distinct) not supported in PartitionEvaluator
      if (((GenericUDAFSumEvaluator)wrappedEvaluator).isWindowingDistinct()) {
        return super.iterate(currentRow, leadLagInfo);
      }

      Range currentRange = getRange(winFrame, currentRow, partition);
      ResultType result;
      if (currentRow == 0 ||  // Reset for the new partition
          sumAgg.prevRange == null ||
          currentRange.getSize() <= currentRange.getDiff(sumAgg.prevRange)) {
        result = (ResultType)calcFunctionValue(currentRange.iterator(), leadLagInfo);
        sumAgg.prevRange = currentRange;
        sumAgg.empty = false;
        sumAgg.prevSum = result;
      } else {
        // Given the previous range and the current range, calculate the new sum
        // from the previous sum and the difference to save the computation.
        Range r1 = new Range(sumAgg.prevRange.start, currentRange.start, partition);
        Range r2 = new Range(sumAgg.prevRange.end, currentRange.end, partition);
        ResultType sum1 = (ResultType)calcFunctionValue(r1.iterator(), leadLagInfo);
        ResultType sum2 = (ResultType)calcFunctionValue(r2.iterator(), leadLagInfo);
        result = typeOperation.add(typeOperation.minus(sumAgg.prevSum, sum1), sum2);

        sumAgg.prevRange = currentRange;
        sumAgg.prevSum = result;
      }

      return result;
    }
  }

  public static class SumPartitionDoubleEvaluator extends SumPartitionEvaluator<DoubleWritable> {
    public SumPartitionDoubleEvaluator(GenericUDAFEvaluator wrappedEvaluator,
        WindowFrameDef winFrame, PTFPartition partition,
        List<PTFExpressionDef> parameters, ObjectInspector outputOI) {
      super(wrappedEvaluator, winFrame, partition, parameters, outputOI);
      this.typeOperation = new TypeOperationDoubleWritable();
    }
  }

  public static class SumPartitionLongEvaluator extends SumPartitionEvaluator<LongWritable> {
    public SumPartitionLongEvaluator(GenericUDAFEvaluator wrappedEvaluator,
        WindowFrameDef winFrame, PTFPartition partition,
        List<PTFExpressionDef> parameters, ObjectInspector outputOI) {
      super(wrappedEvaluator, winFrame, partition, parameters, outputOI);
      this.typeOperation = new TypeOperationLongWritable();
    }
  }

  public static class SumPartitionHiveDecimalEvaluator extends SumPartitionEvaluator<HiveDecimalWritable> {
    public SumPartitionHiveDecimalEvaluator(GenericUDAFEvaluator wrappedEvaluator,
        WindowFrameDef winFrame, PTFPartition partition,
        List<PTFExpressionDef> parameters, ObjectInspector outputOI) {
      super(wrappedEvaluator, winFrame, partition, parameters, outputOI);
      this.typeOperation = new TypeOperationHiveDecimalWritable();
    }
  }

  /**
   * The partition evalulator for average function
   * @param <ResultType>
   */
  public static abstract class AvgPartitionEvaluator<ResultType extends Writable>
  extends BasePartitionEvaluator {
    static class WindowAvgAgg<ResultType> extends AbstractAggregationBuffer {
      Range prevRange;
      ResultType prevSum;
      long prevCount;
      boolean empty;
    }

    protected SumPartitionEvaluator<ResultType> sumEvaluator;
    protected TypeOperationBase<ResultType> typeOperation;
    WindowAvgAgg<ResultType> avgAgg = new WindowAvgAgg<ResultType>();

    public AvgPartitionEvaluator(
        GenericUDAFEvaluator wrappedEvaluator,
        WindowFrameDef winFrame,
        PTFPartition partition,
        List<PTFExpressionDef> parameters,
        ObjectInspector outputOI) {
      super(wrappedEvaluator, winFrame, partition, parameters, outputOI);
    }

    /**
     * Calculate the partial result sum + count giving a parition range
     * @return a 2-element Object array of [count long, sum ResultType]
     */
    private Object[] calcPartialResult(PTFPartitionIterator<Object> pItr, LeadLagInfo leadLagInfo)
        throws HiveException {
      // To handle the case like SUM(LAG(f)) over(), aggregation function includes
      // LAG/LEAD call
      PTFOperator.connectLeadLagFunctionsToPartition(leadLagInfo, pItr);

      AggregationBuffer aggBuffer = wrappedEvaluator.getNewAggregationBuffer();
      Object[] argValues = new Object[parameters == null ? 0 : parameters.size()];
      while(pItr.hasNext())
      {
        Object row = pItr.next();
        int i = 0;
        if ( parameters != null ) {
          for(PTFExpressionDef param : parameters)
          {
            argValues[i++] = param.getExprEvaluator().evaluate(row);
          }
        }
        wrappedEvaluator.aggregate(aggBuffer, argValues);
      }

      // The object [count LongWritable, sum ResultType] is reused during evaluating
      Object[] partial = (Object[])wrappedEvaluator.terminatePartial(aggBuffer);
      return new Object[] {((LongWritable)partial[0]).get(), ObjectInspectorUtils.copyToStandardObject(partial[1], outputOI)};
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public Object iterate(int currentRow, LeadLagInfo leadLagInfo) throws HiveException {
      // // Currently avg(distinct) not supported in PartitionEvaluator
      if (((AbstractGenericUDAFAverageEvaluator)wrappedEvaluator).isWindowingDistinct()) {
        return super.iterate(currentRow, leadLagInfo);
      }

      Range currentRange = getRange(winFrame, currentRow, partition);
      if (currentRow == 0 ||  // Reset for the new partition
          avgAgg.prevRange == null ||
          currentRange.getSize() <= currentRange.getDiff(avgAgg.prevRange)) {
        Object[] partial = (Object[])calcPartialResult(currentRange.iterator(), leadLagInfo);
        avgAgg.prevRange = currentRange;
        avgAgg.empty = false;
        avgAgg.prevSum = (ResultType)partial[1];
        avgAgg.prevCount = (long)partial[0];
      } else {
        // Given the previous range and the current range, calculate the new sum
        // from the previous sum and the difference to save the computation.
        Range r1 = new Range(avgAgg.prevRange.start, currentRange.start, partition);
        Range r2 = new Range(avgAgg.prevRange.end, currentRange.end, partition);
        Object[] partial1 = (Object[])calcPartialResult(r1.iterator(), leadLagInfo);
        Object[] partial2 = (Object[])calcPartialResult(r2.iterator(), leadLagInfo);
        ResultType sum = typeOperation.add(typeOperation.minus(avgAgg.prevSum, (ResultType)partial1[1]), (ResultType)partial2[1]);
        long count = avgAgg.prevCount - (long)partial1[0]+ (long)partial2[0];

        avgAgg.prevRange = currentRange;
        avgAgg.prevSum = sum;
        avgAgg.prevCount = count;
      }

      return typeOperation.div(avgAgg.prevSum, avgAgg.prevCount);
    }
  }

  public static class AvgPartitionDoubleEvaluator extends AvgPartitionEvaluator<DoubleWritable> {

    public AvgPartitionDoubleEvaluator(GenericUDAFEvaluator wrappedEvaluator,
        WindowFrameDef winFrame, PTFPartition partition,
        List<PTFExpressionDef> parameters, ObjectInspector inputOI, ObjectInspector outputOI) throws HiveException {
      super(wrappedEvaluator, winFrame, partition, parameters, outputOI);
      this.typeOperation = new TypeOperationDoubleWritable();
    }
  }

  public static class AvgPartitionHiveDecimalEvaluator extends AvgPartitionEvaluator<HiveDecimalWritable> {

    public AvgPartitionHiveDecimalEvaluator(GenericUDAFEvaluator wrappedEvaluator,
        WindowFrameDef winFrame, PTFPartition partition,
        List<PTFExpressionDef> parameters, ObjectInspector inputOI, ObjectInspector outputOI) throws HiveException {
      super(wrappedEvaluator, winFrame, partition, parameters, outputOI);
      this.typeOperation = new TypeOperationHiveDecimalWritable();
    }
  }
}