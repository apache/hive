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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.exec.WindowFunctionDescription;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.WindowingSpec.BoundarySpec;
import org.apache.hadoop.hive.ql.plan.ptf.BoundaryDef;
import org.apache.hadoop.hive.ql.plan.ptf.WindowFrameDef;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AggregationBuffer;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

@WindowFunctionDescription(description = @Description(name = "last_value", value = "_FUNC_(x)"),
  supportsWindow = true, pivotResult = false, impliesOrder = true)
public class GenericUDAFLastValue extends AbstractGenericUDAFResolver {

  static final Logger LOG = LoggerFactory.getLogger(GenericUDAFLastValue.class.getName());

  @Override
  public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws SemanticException {
    if (parameters.length > 2) {
      throw new UDFArgumentTypeException(2, "At most 2 arguments expected");
    }
    if (parameters.length > 1 && !parameters[1].equals(TypeInfoFactory.booleanTypeInfo)) {
      throw new UDFArgumentTypeException(1, "second argument must be a boolean expression");
    }
    return createEvaluator();
  }

  protected GenericUDAFLastValueEvaluator createEvaluator() {
    return new GenericUDAFLastValueEvaluator();
  }

  static class LastValueBuffer implements AggregationBuffer {

    Object val;
    boolean firstRow;
    boolean skipNulls;

    LastValueBuffer() {
      init();
    }

    void init() {
      val = null;
      firstRow = true;
      skipNulls = false;
    }

  }

  public static class GenericUDAFLastValueEvaluator extends GenericUDAFEvaluator {

    ObjectInspector inputOI;
    ObjectInspector outputOI;

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
      return new LastValueBuffer();
    }

    @Override
    public void reset(AggregationBuffer agg) throws HiveException {
      ((LastValueBuffer) agg).init();
    }

    @Override
    public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
      LastValueBuffer lb = (LastValueBuffer) agg;
      if (lb.firstRow) {
        lb.firstRow = false;
        if (parameters.length == 2) {
          lb.skipNulls = PrimitiveObjectInspectorUtils.getBoolean(parameters[1],
            PrimitiveObjectInspectorFactory.writableBooleanObjectInspector);
        }
      }

      Object o = ObjectInspectorUtils.copyToStandardObject(parameters[0], inputOI,
        ObjectInspectorCopyOption.WRITABLE);

      if (!lb.skipNulls || o != null) {
        lb.val = o;
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
      LastValueBuffer lb = (LastValueBuffer) agg;
      return lb.val;

    }

    @Override
    public GenericUDAFEvaluator getWindowingEvaluator(WindowFrameDef wFrmDef) {
      return new LastValStreamingFixedWindow(this, wFrmDef);
    }
  }

  static class LastValStreamingFixedWindow extends GenericUDAFStreamingEvaluator<Object> {

    class State extends GenericUDAFStreamingEvaluator<Object>.StreamingState {

      private Object lastValue;
      private int lastIdx;

      public State(AggregationBuffer buf) {
        super(buf);
        lastValue = null;
        lastIdx = -1;
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
        return 2 * underlying;
      }

      protected void reset() {
        lastValue = null;
        lastIdx = -1;
        super.reset();
      }
    }

    public LastValStreamingFixedWindow(GenericUDAFEvaluator wrappedEval, WindowFrameDef wFrameDef) {
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
      return ((GenericUDAFLastValueEvaluator) wrappedEval).inputOI;
    }

    @Override
    public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {

      State s = (State) agg;
      LastValueBuffer lb = (LastValueBuffer) s.wrappedBuf;

      /*
       * on firstRow invoke underlying evaluator to initialize skipNulls flag.
       */
      if (lb.firstRow) {
        wrappedEval.iterate(lb, parameters);

        // We need to insert 'null' before processing first row for the case: X preceding and y preceding
        for (int i = wFrameDef.getEnd().getRelativeOffset(); i < 0; i++) {
          s.results.add(null);
        }
      }

      Object o = ObjectInspectorUtils.copyToStandardObject(parameters[0], inputOI(),
        ObjectInspectorCopyOption.WRITABLE);

      if (!lb.skipNulls || o != null) {
        s.lastValue = o;
        s.lastIdx = s.numRows;
      } else if (lb.skipNulls && s.lastIdx != -1) {
        if (!wFrameDef.isStartUnbounded()
            && s.numRows >= s.lastIdx + wFrameDef.getWindowSize()) {
          s.lastValue = null;
          s.lastIdx = -1;
        }
      }

      if (s.numRows >= wFrameDef.getEnd().getRelativeOffset()) {
        s.results.add(s.lastValue);
      }
      s.numRows++;
    }

    @Override
    public Object terminate(AggregationBuffer agg) throws HiveException {
      State s = (State) agg;
      LastValueBuffer lb = (LastValueBuffer) s.wrappedBuf;

      if (lb.skipNulls && s.lastIdx != -1) {
        if (!wFrameDef.isStartUnbounded()
            && s.numRows >= s.lastIdx + wFrameDef.getWindowSize()) {
          s.lastValue = null;
          s.lastIdx = -1;
        }
      }

      // After all the rows are processed, continue to generate results for the rows that results haven't generated.
      // For the case: X following and Y following, process first Y-X results and then insert X nulls.
      // For the case X preceding and Y following, process Y results.
      for (int i = Math.max(0, wFrameDef.getStart().getRelativeOffset()); i < wFrameDef.getEnd().getRelativeOffset(); i++) {
        if (s.hasResultReady()) {
          s.results.add(s.lastValue);
        }
        s.numRows++;
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

