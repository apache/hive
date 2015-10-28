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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.io.IntWritable;

/**
 * abstract class for Lead & lag UDAFs GenericUDAFLeadLag.
 *
 */
public abstract class GenericUDAFLeadLag extends AbstractGenericUDAFResolver {

  static final Logger LOG = LoggerFactory.getLogger(GenericUDAFLead.class.getName());

  @Override
  public GenericUDAFEvaluator getEvaluator(GenericUDAFParameterInfo parameters)
          throws SemanticException {

    ObjectInspector[] paramOIs = parameters.getParameterObjectInspectors();
    String fNm = functionName();

    if (!(paramOIs.length >= 1 && paramOIs.length <= 3)) {
      throw new UDFArgumentTypeException(paramOIs.length - 1, "Incorrect invocation of " + fNm
              + ": _FUNC_(expr, amt, default)");
    }

    int amt = 1;
    if (paramOIs.length > 1) {
      ObjectInspector amtOI = paramOIs[1];

      if (!ObjectInspectorUtils.isConstantObjectInspector(amtOI)
              || (amtOI.getCategory() != ObjectInspector.Category.PRIMITIVE)
              || ((PrimitiveObjectInspector) amtOI).getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.INT) {
        throw new UDFArgumentTypeException(1, fNm + " amount must be a integer value "
                + amtOI.getTypeName() + " was passed as parameter 1.");
      }
      Object o = ((ConstantObjectInspector) amtOI).getWritableConstantValue();
      amt = ((IntWritable) o).get();
      if (amt < 0) {
        throw new UDFArgumentTypeException(1, fNm + " amount can not be nagative. Specified: " + amt );
      }
    }

    if (paramOIs.length == 3) {
      ObjectInspectorConverters.getConverter(paramOIs[2], paramOIs[0]);
    }

    GenericUDAFLeadLagEvaluator eval = createLLEvaluator();
    eval.setAmt(amt);
    return eval;
  }

  protected abstract String functionName();

  protected abstract GenericUDAFLeadLagEvaluator createLLEvaluator();

  public static abstract class GenericUDAFLeadLagEvaluator extends GenericUDAFEvaluator {

    private transient ObjectInspector[] inputOI;
    private int amt;
    String fnName;
    private transient Converter defaultValueConverter;

    public GenericUDAFLeadLagEvaluator() {
    }

    /*
     * used to initialize Streaming Evaluator.
     */
    protected GenericUDAFLeadLagEvaluator(GenericUDAFLeadLagEvaluator src) {
      this.inputOI = src.inputOI;
      this.amt = src.amt;
      this.fnName = src.fnName;
      this.defaultValueConverter = src.defaultValueConverter;
      this.mode = src.mode;
    }

    @Override
    public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
      super.init(m, parameters);
      if (m != Mode.COMPLETE) {
        throw new HiveException("Only COMPLETE mode supported for " + fnName + " function");
      }

      inputOI = parameters;

      if (parameters.length == 3) {
        defaultValueConverter = ObjectInspectorConverters
                .getConverter(parameters[2], parameters[0]);
      }

      return ObjectInspectorFactory.getStandardListObjectInspector(ObjectInspectorUtils
              .getStandardObjectInspector(parameters[0]));
    }

    public int getAmt() {
      return amt;
    }

    public void setAmt(int amt) {
      this.amt = amt;
    }

    public String getFnName() {
      return fnName;
    }

    public void setFnName(String fnName) {
      this.fnName = fnName;
    }

    protected abstract LeadLagBuffer getNewLLBuffer() throws HiveException;

    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
      LeadLagBuffer lb = getNewLLBuffer();
      lb.initialize(amt);
      return lb;
    }

    @Override
    public void reset(AggregationBuffer agg) throws HiveException {
      ((LeadLagBuffer) agg).initialize(amt);
    }

    @Override
    public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
      Object rowExprVal = ObjectInspectorUtils.copyToStandardObject(parameters[0], inputOI[0]);
      Object defaultVal = parameters.length > 2 ? ObjectInspectorUtils.copyToStandardObject(
              defaultValueConverter.convert(parameters[2]), inputOI[0]) : null;
      ((LeadLagBuffer) agg).addRow(rowExprVal, defaultVal);
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
      return ((LeadLagBuffer) agg).terminate();
    }

  }

}
