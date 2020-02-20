/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.datasketches.hive.hll;

import org.apache.datasketches.hll.HllSketch;
import org.apache.datasketches.hll.TgtHllType;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AggregationBuffer;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;

@Description(
    name = "SketchToEstimate2",
    value = "_FUNC_(sketch)",
    extended = "Returns an estimate of unique count from a given HllSketch."
    + " The result is a double value.")
@SuppressWarnings("javadoc")
public class SketchToEstimateUDF2 extends GenericUDF {

  //  /**
  //   * Get an estimate from a given HllSketch
  //   * @param serializedSketch HllSketch in a serialized binary form
  //   * @return estimate of unique count
  //   */
  //  public Double evaluate(final BytesWritable serializedSketch) {
  //    if (serializedSketch == null) { return null; }
  //    final HllSketch sketch = HllSketch.wrap(Memory.wrap(serializedSketch.getBytes()));
  //    return sketch.getEstimate();
  //  }

  static class X extends SketchEvaluator2 {

    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
      return new UnionState();
    }

    @Override
    public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
      if (parameters[0] == null) {
        return;
      }
      final State state = (State) agg;
      if (!state.isInitialized()) {
        initializeState(state, parameters);
      }
      state.update(parameters[0], inputInspector_);
    }

    private void initializeState(final State state, final Object[] parameters) {
      int lgK = DEFAULT_LG_K;
      if (lgKInspector_ != null) {
        lgK = PrimitiveObjectInspectorUtils.getInt(parameters[1], lgKInspector_);
      }
      TgtHllType type = DEFAULT_HLL_TYPE;
      if (hllTypeInspector_ != null) {
        type = TgtHllType.valueOf(PrimitiveObjectInspectorUtils.getString(parameters[2], hllTypeInspector_));
      }
      state.init(lgK, type);
    }

  }

  X x;
  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    x = new X();
    x.intermediateInspector_ = (StructObjectInspector) arguments[0];
    return PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveCategory.DOUBLE);
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    AggregationBuffer buf = x.getNewAggregationBuffer();
    Object data = arguments[0];
    if (data instanceof DeferredJavaObject) {
      try {
        data = ((DeferredJavaObject) data).get();
      } catch (HiveException e) {
        throw new RuntimeException(e);
      }
    }

    x.merge(buf, data);
    final HllSketch result = ((State) buf).getResult();

    return result.getEstimate();
  }

  @Override
  public String getDisplayString(String[] children) {
    return "sketchTOEstimate2xxxxFIXME";
  }

}
