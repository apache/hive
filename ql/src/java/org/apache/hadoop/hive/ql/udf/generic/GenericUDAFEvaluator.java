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

import java.io.Closeable;
import java.io.IOException;

import org.apache.hadoop.hive.ql.exec.MapredContext;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

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
@UDFType(deterministic = true)
public abstract class GenericUDAFEvaluator implements Closeable {

  public static @interface AggregationType {
    boolean estimable() default false;
  }

  public static boolean isEstimable(AggregationBuffer buffer) {
    if (buffer instanceof AbstractAggregationBuffer) {
      Class<? extends AggregationBuffer> clazz = buffer.getClass();
      AggregationType annotation = clazz.getAnnotation(AggregationType.class);
      return annotation != null && annotation.estimable();
    }
    return false;
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
   * @param context context
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
  public static interface AggregationBuffer {
  };

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

}
