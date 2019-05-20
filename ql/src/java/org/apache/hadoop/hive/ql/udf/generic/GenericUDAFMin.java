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
import org.apache.hadoop.hive.ql.exec.vector.VectorizedUDAFs;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.gen.*;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ptf.BoundaryDef;
import org.apache.hadoop.hive.ql.plan.ptf.WindowFrameDef;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AggregationType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFMax.MaxStreamingFixedWindow;
import org.apache.hadoop.hive.ql.util.JavaDataModel;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.FullMapEqualComparer;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.NullValueOption;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

@Description(name = "min", value = "_FUNC_(expr) - Returns the minimum value of expr")
public class GenericUDAFMin extends AbstractGenericUDAFResolver {

  static final Logger LOG = LoggerFactory.getLogger(GenericUDAFMin.class.getName());

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
    return new GenericUDAFMinEvaluator();
  }

  @UDFType(distinctLike=true)
  @VectorizedUDAFs({
    VectorUDAFMinLong.class,
    VectorUDAFMinDouble.class,
    VectorUDAFMinDecimal.class,
    VectorUDAFMinDecimal64.class,
    VectorUDAFMinTimestamp.class,
    VectorUDAFMinIntervalDayTime.class,
    VectorUDAFMinString.class})
  public static class GenericUDAFMinEvaluator extends GenericUDAFEvaluator {

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
    static class MinAgg extends AbstractAggregationBuffer {
      Object o;
      @Override
      public int estimate() {
        return JavaDataModel.PRIMITIVES2;
      }
    }

    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
      MinAgg result = new MinAgg();
      return result;
    }

    @Override
    public void reset(AggregationBuffer agg) throws HiveException {
      MinAgg myagg = (MinAgg) agg;
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
        MinAgg myagg = (MinAgg) agg;
        int r = ObjectInspectorUtils.compare(myagg.o, outputOI, partial, inputOI, new FullMapEqualComparer(), NullValueOption.MAXVALUE);
        if (myagg.o == null || r > 0) {
          myagg.o = ObjectInspectorUtils.copyToStandardObject(partial, inputOI,
              ObjectInspectorCopyOption.JAVA);
        }
      }
    }

    @Override
    public Object terminate(AggregationBuffer agg) throws HiveException {
      MinAgg myagg = (MinAgg) agg;
      return myagg.o;
    }

    @Override
    public GenericUDAFEvaluator getWindowingEvaluator(WindowFrameDef wFrmDef) {
      return new MinStreamingFixedWindow(this, wFrmDef);
    }

  }

  static class MinStreamingFixedWindow extends MaxStreamingFixedWindow {

    public MinStreamingFixedWindow(GenericUDAFEvaluator wrappedEval,
        WindowFrameDef wFrmDef) {
      super(wrappedEval, wFrmDef);
    }

    @Override
    protected ObjectInspector inputOI() {
      return ((GenericUDAFMinEvaluator) wrappedEval).inputOI;
    }

    @Override
    protected ObjectInspector outputOI() {
      return ((GenericUDAFMinEvaluator) wrappedEval).outputOI;
    }

    @Override
    protected boolean removeLast(Object in, Object last) {
      return isLess(in, last);
    }

    private boolean isLess(Object in, Object last) {
      if (in == null) {
        return false;
      }
      if (last == null) {
        return true;
      }
      return ObjectInspectorUtils.compare(in, inputOI(), last, outputOI()) < 0;
    }

  }

}
