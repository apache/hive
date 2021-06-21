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
import org.apache.hadoop.hive.ql.exec.WindowFunctionDescription;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ptf.WindowFrameDef;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AggregationBuffer;
import org.apache.hadoop.hive.ql.util.JavaDataModel;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

@Description(
        name = "first_value",
        value = "_FUNC_(x)")
@WindowFunctionDescription(
        supportsWindow = true,
        pivotResult = false,
        impliesOrder = true,
        supportsNullTreatment = true)
public class GenericUDAFFirstValue extends AbstractGenericUDAFResolver {

  static final Logger LOG = LoggerFactory.getLogger(GenericUDAFFirstValue.class.getName());

  @Override
  public GenericUDAFEvaluator getEvaluator(GenericUDAFParameterInfo info) throws SemanticException {
    TypeInfo[] parameters = info.getParameters();
    if (parameters.length > 2) {
      throw new UDFArgumentTypeException(2, "At most 2 arguments expected");
    }
    if (parameters.length > 1 && !parameters[1].equals(TypeInfoFactory.booleanTypeInfo)) {
      throw new UDFArgumentTypeException(1, "second argument must be a boolean expression");
    }
    return createEvaluator(!info.respectNulls());
  }

  protected GenericUDAFFirstValueEvaluator createEvaluator(boolean skipNulls) {
    return new GenericUDAFFirstValueEvaluator(skipNulls);
  }

  static class FirstValueBuffer implements AggregationBuffer {

    Object val;
    boolean valSet;
    boolean firstRow;
    boolean skipNulls;

    FirstValueBuffer(boolean skipNulls) {
      init(skipNulls);
    }

    void init(boolean skipNulls) {
      val = null;
      valSet = false;
      firstRow = true;
      this.skipNulls = skipNulls;
    }

  }

  public static class GenericUDAFFirstValueEvaluator extends GenericUDAFEvaluator {

    ObjectInspector inputOI;
    ObjectInspector outputOI;
    final boolean skipNulls;

    public GenericUDAFFirstValueEvaluator(boolean skipNulls) {
      this.skipNulls = skipNulls;
    }

    @Override
    public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
      super.init(m, parameters);
      if (m != Mode.COMPLETE) {
        throw new HiveException("Only COMPLETE mode supported for Rank function");
      }
      inputOI = parameters[0];
      outputOI = ObjectInspectorUtils.getStandardObjectInspector(inputOI,
        ObjectInspectorCopyOption.WRITABLE);
      return outputOI;
    }

    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
      return new FirstValueBuffer(skipNulls);
    }

    @Override
    public void reset(AggregationBuffer agg) throws HiveException {
      ((FirstValueBuffer) agg).init(skipNulls);
    }

    @Override
    public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
      FirstValueBuffer fb = (FirstValueBuffer) agg;

      if (fb.firstRow) {
        fb.firstRow = false;
        if (parameters.length == 2) {
          fb.skipNulls = PrimitiveObjectInspectorUtils.getBoolean(parameters[1],
            PrimitiveObjectInspectorFactory.writableBooleanObjectInspector);
        }
      }

      if (!fb.valSet) {
        fb.val = ObjectInspectorUtils.copyToStandardObject(parameters[0], inputOI,
          ObjectInspectorCopyOption.WRITABLE);
        if (!fb.skipNulls || fb.val != null) {
          fb.valSet = true;
        }
      }
    }

    @Override
    public Object terminatePartial(AggregationBuffer agg) throws HiveException {
      throw new HiveException("terminatePartial not supported");
    }

    @Override
    public void merge(AggregationBuffer agg, Object partial) throws HiveException {
      throw new HiveException("merge not supported");
    }

    @Override
    public Object terminate(AggregationBuffer agg) throws HiveException {
      return ((FirstValueBuffer) agg).val;
    }

    @Override
    public GenericUDAFEvaluator getWindowingEvaluator(WindowFrameDef wFrmDef) {
      return new FirstValStreamingFixedWindow(this, wFrmDef);
    }

  }

  static class ValIndexPair {

    Object val;
    int idx;

    ValIndexPair(Object val, int idx) {
      this.val = val;
      this.idx = idx;
    }
  }

  static class FirstValStreamingFixedWindow extends GenericUDAFStreamingEvaluator<Object> {

    class State extends GenericUDAFStreamingEvaluator<Object>.StreamingState {

      private final Deque<ValIndexPair> valueChain;

      public State(AggregationBuffer buf) {
        super(buf);
        valueChain = new ArrayDeque<ValIndexPair>(wFrameDef.isStartUnbounded() ? 1 : wFrameDef.getWindowSize());
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
        return underlying + (underlying * wdwSz) + (underlying * wdwSz) + (3
                                                                           * JavaDataModel.PRIMITIVES1);
      }

      protected void reset() {
        valueChain.clear();
        super.reset();
      }
    }

    public FirstValStreamingFixedWindow(GenericUDAFEvaluator wrappedEval, WindowFrameDef wFrameDef) {
      super(wrappedEval, wFrameDef);
    }

    @Override
    public int getRowsRemainingAfterTerminate() throws HiveException {
      throw new UnsupportedOperationException();
    }

    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
      AggregationBuffer underlying = wrappedEval.getNewAggregationBuffer();
      return new State(underlying);
    }

    protected ObjectInspector inputOI() {
      return ((GenericUDAFFirstValueEvaluator) wrappedEval).inputOI;
    }

    @Override
    public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {

      State s = (State) agg;
      FirstValueBuffer fb = (FirstValueBuffer) s.wrappedBuf;

      /*
       * on firstRow invoke underlying evaluator to initialize skipNulls flag.
       */
      if (fb.firstRow) {
        wrappedEval.iterate(fb, parameters);
        // We need to insert 'null' before processing first row for the case: X preceding and y preceding
        for (int i = wFrameDef.getEnd().getRelativeOffset(); i < 0; i++) {
          s.results.add(null);
        }
      }

      Object o = ObjectInspectorUtils.copyToStandardObject(parameters[0], inputOI(),
        ObjectInspectorCopyOption.WRITABLE);

      /*
       * add row to chain. except in case of UNB preceding: - only 1 firstVal
       * needs to be tracked.
       */
      if (!wFrameDef.isStartUnbounded() || s.valueChain.isEmpty()) {
        /*
         * add value to chain if it is not null or if skipNulls is false.
         */
        if (!fb.skipNulls || o != null) {
          s.valueChain.add(new ValIndexPair(o, s.numRows));
        }
      }

      if (s.hasResultReady()) {
        /*
         * if skipNulls is true and there are no rows in valueChain => all rows
         * in partition are null so far; so add null in o/p
         */
        if (fb.skipNulls && s.valueChain.size() == 0) {
          s.results.add(null);
        } else {
          s.results.add(s.valueChain.getFirst().val);
        }
      }
      s.numRows++;

      if (s.valueChain.size() > 0) {
        int fIdx = (Integer) s.valueChain.getFirst().idx;
        if (!wFrameDef.isStartUnbounded()
            && s.numRows >= fIdx +  wFrameDef.getWindowSize()) {
          s.valueChain.removeFirst();
        }
      }
    }

    @Override
    public Object terminate(AggregationBuffer agg) throws HiveException {
      State s = (State) agg;
      ValIndexPair r = s.valueChain.size() == 0 ? null : s.valueChain.getFirst();

      // After all the rows are processed, continue to generate results for the rows that results haven't generated.
      // For the case: X following and Y following, process first Y-X results and then insert X nulls.
      // For the case X preceding and Y following, process Y results.
      for (int i = Math.max(0, wFrameDef.getStart().getRelativeOffset()); i < wFrameDef.getEnd().getRelativeOffset(); i++) {
        if (s.hasResultReady()) {
          s.results.add(r == null ? null : r.val);
        }
        s.numRows++;
        if (r != null) {
          int fIdx = (Integer) r.idx;
          if (!wFrameDef.isStartUnbounded()
              && s.numRows >= fIdx + wFrameDef.getWindowSize()
              && !s.valueChain.isEmpty()) {
            s.valueChain.removeFirst();
            r = !s.valueChain.isEmpty() ? s.valueChain.getFirst() : r;
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

  }
}

