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

import java.util.ArrayDeque;
import java.util.Deque;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedUDAFs;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.gen.*;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.WindowingSpec.BoundarySpec;
import org.apache.hadoop.hive.ql.plan.ptf.BoundaryDef;
import org.apache.hadoop.hive.ql.plan.ptf.WindowFrameDef;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AggregationBuffer;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AggregationType;
import org.apache.hadoop.hive.ql.util.JavaDataModel;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

@Description(name = "max", value = "_FUNC_(expr) - Returns the maximum value of expr")
public class GenericUDAFMax extends AbstractGenericUDAFResolver {

  static final Logger LOG = LoggerFactory.getLogger(GenericUDAFMax.class.getName());

  @Override
  public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters)
    throws SemanticException {
    if (parameters.length != 1) {
      throw new UDFArgumentTypeException(parameters.length - 1,
          "Exactly one argument is expected.");
    }
    ObjectInspector oi = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(parameters[0]);
    if (!ObjectInspectorUtils.compareSupported(oi)) {
      throw new UDFArgumentTypeException(parameters.length - 1,
          "Cannot support comparison of map<> type or complex type containing map<>.");
    }
    return new GenericUDAFMaxEvaluator();
  }

  @UDFType(distinctLike=true)
  @VectorizedUDAFs({
    VectorUDAFMaxLong.class,
    VectorUDAFMaxDouble.class,
    VectorUDAFMaxDecimal.class,
    VectorUDAFMaxDecimal64.class,
    VectorUDAFMaxTimestamp.class,
    VectorUDAFMaxIntervalDayTime.class,
    VectorUDAFMaxString.class})
  public static class GenericUDAFMaxEvaluator extends GenericUDAFEvaluator {

    private transient ObjectInspector inputOI;
    private transient ObjectInspector outputOI;

    @Override
    public ObjectInspector init(Mode m, ObjectInspector[] parameters)
        throws HiveException {
      assert (parameters.length == 1);
      super.init(m, parameters);
      inputOI = parameters[0];
      // Copy to Java object because that saves object creation time.
      // Note that on average the number of copies is log(N) so that's not
      // very important.
      outputOI = ObjectInspectorUtils.getStandardObjectInspector(inputOI,
          ObjectInspectorCopyOption.JAVA);
      return outputOI;
    }

    /** class for storing the current max value */
    @AggregationType(estimable = true)
    static class MaxAgg extends AbstractAggregationBuffer {
      Object o;
      @Override
      public int estimate() {
        return JavaDataModel.PRIMITIVES2;
      }
    }

    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
      MaxAgg result = new MaxAgg();
      return result;
    }

    @Override
    public void reset(AggregationBuffer agg) throws HiveException {
      MaxAgg myagg = (MaxAgg) agg;
      myagg.o = null;
    }

    boolean warned = false;

    @Override
    public void iterate(AggregationBuffer agg, Object[] parameters)
        throws HiveException {
      assert (parameters.length == 1);
      merge(agg, parameters[0]);
    }

    @Override
    public Object terminatePartial(AggregationBuffer agg) throws HiveException {
      return terminate(agg);
    }

    @Override
    public void merge(AggregationBuffer agg, Object partial)
        throws HiveException {
      if (partial != null) {
        MaxAgg myagg = (MaxAgg) agg;
        int r = ObjectInspectorUtils.compare(myagg.o, outputOI, partial, inputOI);
        if (myagg.o == null || r < 0) {
          myagg.o = ObjectInspectorUtils.copyToStandardObject(partial, inputOI,
              ObjectInspectorCopyOption.JAVA);
        }
      }
    }

    @Override
    public Object terminate(AggregationBuffer agg) throws HiveException {
      MaxAgg myagg = (MaxAgg) agg;
      return myagg.o;
    }

    @Override
    public GenericUDAFEvaluator getWindowingEvaluator(WindowFrameDef wFrmDef) {
      return new MaxStreamingFixedWindow(this, wFrmDef);
    }

  }

  /*
   * Based on the Paper by Daniel Lemire: Streaming Max-Min filter using no more
   * than 3 comparisons per elem.
   *
   * 1. His algorithm works on fixed size windows up to the current row. For row
   * 'i' and window 'w' it computes the min/max for window (i-w, i). 2. The core
   * idea is to keep a queue of (max, idx) tuples. A tuple in the queue
   * represents the max value in the range (prev tuple.idx, idx). Using the
   * queue data structure and following 2 operations it is easy to see that
   * maxes can be computed: - on receiving the ith row; drain the queue from the
   * back of any entries whose value is less than the ith entry; add the ith
   * value as a tuple in the queue (i-val, i) - on the ith step, check if the
   * element at the front of the queue has reached its max range of influence;
   * i.e. frontTuple.idx + w > i. If yes we can remove it from the queue. - on
   * the ith step o/p the front of the queue as the max for the ith entry.
   *
   * Here we modify the algorithm: 1. to handle window's that are of the form
   * (i-p, i+f), where p is numPreceding,f = numFollowing - we start outputting
   * rows only after receiving f rows. - the formula for 'influence range' of an
   * idx accounts for the following rows. 2. optimize for the case when
   * numPreceding is Unbounded. In this case only 1 max needs to be tracked at
   * any given time.
   */
  static class MaxStreamingFixedWindow extends
      GenericUDAFStreamingEvaluator<Object> {

    class State extends GenericUDAFStreamingEvaluator<Object>.StreamingState {
      private final Deque<Object[]> maxChain;

      public State(AggregationBuffer buf) {
        super(buf);
        maxChain = new ArrayDeque<Object[]>(wFrameDef.isStartUnbounded() ? 1 : wFrameDef.getWindowSize());
      }

      @Override
      public int estimate() {
        if (!(wrappedBuf instanceof AbstractAggregationBuffer)) {
          return -1;
        }
        int underlying = ((AbstractAggregationBuffer) wrappedBuf).estimate();
        if (underlying == -1) {
          return -1;
        }
        if (wFrameDef.isStartUnbounded()) {
          return -1;
        }
        /*
         * sz Estimate = sz needed by underlying AggBuffer + sz for results + sz
         * for maxChain + 3 * JavaDataModel.PRIMITIVES1 sz of results = sz of
         * underlying * wdwSz sz of maxChain = sz of underlying * wdwSz
         */

        int wdwSz = wFrameDef.getWindowSize();
        return underlying + (underlying * wdwSz) + (underlying * wdwSz)
            + (3 * JavaDataModel.PRIMITIVES1);
      }

      @Override
      protected void reset() {
        maxChain.clear();
        super.reset();
      }

    }

    public MaxStreamingFixedWindow(GenericUDAFEvaluator wrappedEval,
        WindowFrameDef wFrmDef) {
      super(wrappedEval, wFrmDef);
    }

    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
      AggregationBuffer underlying = wrappedEval.getNewAggregationBuffer();
      return new State(underlying);
    }

    protected ObjectInspector inputOI() {
      return ((GenericUDAFMaxEvaluator) wrappedEval).inputOI;
    }

    protected ObjectInspector outputOI() {
      return ((GenericUDAFMaxEvaluator) wrappedEval).outputOI;
    }

    @Override
    public void iterate(AggregationBuffer agg, Object[] parameters)
        throws HiveException {

      State s = (State) agg;
      Object o = parameters[0];

      while (!s.maxChain.isEmpty()) {
        if (!removeLast(o, s.maxChain.getLast()[0])) {
          break;
        } else {
          s.maxChain.removeLast();
        }
      }

      // We need to insert 'null' before processing first row for the case: X preceding and y preceding
      if (s.numRows == 0) {
        for (int i = wFrameDef.getEnd().getRelativeOffset(); i < 0; i++) {
          s.results.add(null);
        }
      }

      /*
       * add row to chain. except in case of UNB preceding: - only 1 max needs
       * to be tracked. - current max will never become out of range. It can
       * only be replaced by a larger max.
       */
      if (!wFrameDef.isStartUnbounded() || s.maxChain.isEmpty()) {
        o = o == null ? null : ObjectInspectorUtils.copyToStandardObject(o,
            inputOI(), ObjectInspectorCopyOption.JAVA);
        s.maxChain.addLast(new Object[] { o, s.numRows });
      }

      if (s.hasResultReady()) {
        s.results.add(s.maxChain.getFirst()[0]);
      }
      s.numRows++;

      int fIdx = (Integer) s.maxChain.getFirst()[1];
      if (!wFrameDef.isStartUnbounded()
          && s.numRows >= fIdx +  wFrameDef.getWindowSize()) {
        s.maxChain.removeFirst();
      }
    }

    protected boolean removeLast(Object in, Object last) {
      return isGreater(in, last);
    }

    private boolean isGreater(Object in, Object last) {
      if (in == null) {
        return false;
      }
      if (last == null) {
        return true;
      }
      return ObjectInspectorUtils.compare(in, inputOI(), last, outputOI()) > 0;
    }

    @Override
    public Object terminate(AggregationBuffer agg) throws HiveException {

      State s = (State) agg;
      Object[] r = s.maxChain.isEmpty() ? null : s.maxChain.getFirst();

      // After all the rows are processed, continue to generate results for the rows that results haven't generated.
      // For the case: X following and Y following, process first Y-X results and then insert X nulls.
      // For the case X preceding and Y following, process Y results.
      for (int i = Math.max(0, wFrameDef.getStart().getRelativeOffset()); i < wFrameDef.getEnd().getRelativeOffset(); i++) {
        if (s.hasResultReady()) {
          s.results.add(r == null ? null : r[0]);
        }
        s.numRows++;
        if (r != null) {
          int fIdx = (Integer) r[1];
          if (!wFrameDef.isStartUnbounded()
              && s.numRows >= fIdx + wFrameDef.getWindowSize()
              && !s.maxChain.isEmpty()) {
            s.maxChain.removeFirst();
            r = !s.maxChain.isEmpty() ? s.maxChain.getFirst() : null;
          }
        }
      }
      for (int i = 0; i < wFrameDef.getStart().getRelativeOffset(); i++) {
        if (s.hasResultReady()) {
          s.results.add(null);
        }
        s.numRows++;
      }

      return null;
    }

    @Override
    public int getRowsRemainingAfterTerminate() throws HiveException {
      throw new UnsupportedOperationException();
    }

  }

}
