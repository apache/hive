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

import java.io.Closeable;
import java.io.IOException;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.util.List;

import org.apache.hadoop.hive.common.classification.InterfaceAudience;
import org.apache.hadoop.hive.common.classification.InterfaceStability;
import org.apache.hadoop.hive.ql.exec.MapredContext;
import org.apache.hadoop.hive.ql.exec.PTFPartition;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.ptf.PTFExpressionDef;
import org.apache.hadoop.hive.ql.plan.ptf.WindowFrameDef;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.ptf.BasePartitionEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hive.common.util.AnnotationUtils;

/**
 * A Generic User-defined aggregation function (GenericUDAF) for the use with
 * Hive.
 *
 * New GenericUDAF classes need to inherit from this GenericUDAF class.
 *
 * The GenericUDAF are superior to normal UDAFs in the following ways: 1. It can
 * accept arguments of complex types, and return complex types. 2. It can accept
 * variable length of arguments. 3. It can accept an infinite number of function
 * signature - for example, it's easy to write a GenericUDAF that accepts
 * array<int>, array<array<int>> and so on (arbitrary levels of nesting).
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
@UDFType(deterministic = true)
public abstract class GenericUDAFEvaluator implements Closeable {

  @InterfaceAudience.Public
  @InterfaceStability.Stable
  @Retention(RetentionPolicy.RUNTIME)
  public static @interface AggregationType {
    boolean estimable() default false;
  }

  public static boolean isEstimable(AggregationBuffer buffer) {
    if (buffer instanceof AbstractAggregationBuffer) {
      Class<? extends AggregationBuffer> clazz = buffer.getClass();
      AggregationType annotation = AnnotationUtils.getAnnotation(clazz, AggregationType.class);
      return annotation != null && annotation.estimable();
    }
    return false;
  }

  /**
   * Although similar to AbstractAggregationBuffer::estimate(), it differs from it in 2 aspects
   * 1) This avoids creation of AggregationBuffer which may result in large memory allocation
   * 2) This is used only while compiling query as oppose to AbstractAggregationBuffer version
   * which may be used in both runtime as well as compile time.
   * @return
   */
  public int estimate() {
    return -1;
  }

  /**
   * Mode.
   *
   */
  public static enum Mode {
    /**
     * PARTIAL1: from original data to partial aggregation data: iterate() and
     * terminatePartial() will be called.
     */
    PARTIAL1,
        /**
     * PARTIAL2: from partial aggregation data to partial aggregation data:
     * merge() and terminatePartial() will be called.
     */
    PARTIAL2,
        /**
     * FINAL: from partial aggregation to full aggregation: merge() and
     * terminate() will be called.
     */
    FINAL,
        /**
     * COMPLETE: from original data directly to full aggregation: iterate() and
     * terminate() will be called.
     */
    COMPLETE
  };

  Mode mode;

  /**
   * The constructor.
   */
  public GenericUDAFEvaluator() {
  }

  /**
   * Additionally setup GenericUDAFEvaluator with MapredContext before initializing.
   * This is only called in runtime of MapRedTask.
   *
   * @param mapredContext context
   */
  public void configure(MapredContext mapredContext) {
  }

  /**
   * Initialize the evaluator.
   *
   * @param m
   *          The mode of aggregation.
   * @param parameters
   *          The ObjectInspector for the parameters: In PARTIAL1 and COMPLETE
   *          mode, the parameters are original data; In PARTIAL2 and FINAL
   *          mode, the parameters are just partial aggregations (in that case,
   *          the array will always have a single element).
   * @return The ObjectInspector for the return value. In PARTIAL1 and PARTIAL2
   *         mode, the ObjectInspector for the return value of
   *         terminatePartial() call; In FINAL and COMPLETE mode, the
   *         ObjectInspector for the return value of terminate() call.
   *
   *         NOTE: We need ObjectInspector[] (in addition to the TypeInfo[] in
   *         GenericUDAFResolver) for 2 reasons: 1. ObjectInspector contains
   *         more information than TypeInfo; and GenericUDAFEvaluator.init at
   *         execution time. 2. We call GenericUDAFResolver.getEvaluator at
   *         compilation time,
   */
  public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
    // This function should be overriden in every sub class
    // And the sub class should call super.init(m, parameters) to get mode set.
    mode = m;
    partitionEvaluator = null;
    return null;
  }

  /**
   * The interface for a class that is used to store the aggregation result
   * during the process of aggregation.
   *
   * We split this piece of data out because there can be millions of instances
   * of this Aggregation in hash-based aggregation process, and it's very
   * important to conserve memory.
   *
   * In the future, we may completely hide this class inside the Evaluator and
   * use integer numbers to identify which aggregation we are looking at.
   *
   * @deprecated use {@link AbstractAggregationBuffer} instead
   */
  @Deprecated
  public static interface AggregationBuffer {
  };

  @InterfaceAudience.Public
  @InterfaceStability.Stable
  public static abstract class AbstractAggregationBuffer implements AggregationBuffer {
    /**
     * Estimate the size of memory which is occupied by aggregation buffer.
     * Currently, hive assumes that primitives types occupies 16 byte and java object has
     * 64 byte overhead for each. For map, each entry also has 64 byte overhead.
     */
    public int estimate() { return -1; }
  }

  /**
   * Get a new aggregation object.
   */
  public abstract AggregationBuffer getNewAggregationBuffer() throws HiveException;

  /**
   * Reset the aggregation. This is useful if we want to reuse the same
   * aggregation.
   */
  public abstract void reset(AggregationBuffer agg) throws HiveException;

  /**
   * Close GenericUDFEvaluator.
   * This is only called in runtime of MapRedTask.
   */
  @Override
  public void close() throws IOException {
  }

  /**
   * This function will be called by GroupByOperator when it sees a new input
   * row.
   *
   * @param agg
   *          The object to store the aggregation result.
   * @param parameters
   *          The row, can be inspected by the OIs passed in init().
   */
  public void aggregate(AggregationBuffer agg, Object[] parameters) throws HiveException {
    if (mode == Mode.PARTIAL1 || mode == Mode.COMPLETE) {
      iterate(agg, parameters);
    } else {
      assert (parameters.length == 1);
      merge(agg, parameters[0]);
    }
  }

  /**
   * This function will be called by GroupByOperator when it sees a new input
   * row.
   *
   * @param agg
   *          The object to store the aggregation result.
   */
  public Object evaluate(AggregationBuffer agg) throws HiveException {
    if (mode == Mode.PARTIAL1 || mode == Mode.PARTIAL2) {
      return terminatePartial(agg);
    } else {
      return terminate(agg);
    }
  }

  /**
   * Iterate through original data.
   *
   * @param parameters
   *          The objects of parameters.
   */
  public abstract void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException;

  /**
   * Get partial aggregation result.
   *
   * @return partial aggregation result.
   */
  public abstract Object terminatePartial(AggregationBuffer agg) throws HiveException;

  /**
   * Merge with partial aggregation result. NOTE: null might be passed in case
   * there is no input data.
   *
   * @param partial
   *          The partial aggregation result.
   */
  public abstract void merge(AggregationBuffer agg, Object partial) throws HiveException;

  /**
   * Get final aggregation result.
   *
   * @return final aggregation result.
   */
  public abstract Object terminate(AggregationBuffer agg) throws HiveException;

  /**
   * When evaluating an aggregates over a fixed Window, the naive way to compute
   * results is to compute the aggregate for each row. But often there is a way
   * to compute results in a more efficient manner. This method enables the
   * basic evaluator to provide a function object that does the job in a more
   * efficient manner.
   * <p>
   * This method is called after this Evaluator is initialized. The returned
   * Function must be initialized. It is passed the 'window' of aggregation for
   * each row.
   *
   * @param wFrmDef
   *          the Window definition in play for this evaluation.
   * @return null implies that this fn cannot be processed in Streaming mode. So
   *         each row is evaluated independently.
   */
  public GenericUDAFEvaluator getWindowingEvaluator(WindowFrameDef wFrmDef) {
    return null;
  }

  /**
   * Optional information to add to expression string. Subclasses can override.
   */
  public String getExprString() {
    return "";
  }

  protected BasePartitionEvaluator partitionEvaluator;

  /**
   * When evaluating an aggregates over a fixed Window, streaming is not possible
   * especially for RANGE Window type. For such case, the whole partition data needs
   * to be collected and then to evaluate the aggregates. The naive approach is to
   * calculate a row range for each row and to perform the aggregates. For some
   * functions, a better implementation can be used to reduce the calculation.
   * Note: since the evaluator is reused across different partitions, AggregationBuffer
   * needs reset before aggregating for the new partition in the implementation.
   * @param winFrame    the Window definition in play for this evaluation.
   * @param partition   the partition data
   * @param parameters  the list of the expressions in the function
   * @param outputOI    the output object inspector
   * @param nullsLast   the nulls last configuration
   * @return            the evaluator, default to BasePartitionEvaluator which
   *                    implements the naive approach
   */
  public final BasePartitionEvaluator getPartitionWindowingEvaluator(
      WindowFrameDef winFrame,
      PTFPartition partition,
      List<PTFExpressionDef> parameters,
      ObjectInspector outputOI, boolean nullsLast) {
    if (partitionEvaluator == null) {
      partitionEvaluator = createPartitionEvaluator(winFrame, partition, parameters, outputOI,
          nullsLast);
    }

    return partitionEvaluator;
  }

  /**
   *  This class needs to be overridden by the child class to implement function
   *  specific evaluator.
   */
  protected BasePartitionEvaluator createPartitionEvaluator(
      WindowFrameDef winFrame,
      PTFPartition partition,
      List<PTFExpressionDef> parameters,
      ObjectInspector outputOI,
      boolean nullsLast) {
    return new BasePartitionEvaluator(this, winFrame, partition, parameters, outputOI, nullsLast);
  }
}
