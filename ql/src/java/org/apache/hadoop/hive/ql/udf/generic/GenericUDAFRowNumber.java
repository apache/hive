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

import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.exec.WindowFunctionDescription;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AggregationBuffer;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.IntWritable;

@WindowFunctionDescription(
  description = @Description(
    name = "row_number",
    value = "_FUNC_() - The ROW_NUMBER function assigns a unique number (sequentially, starting "
            + "from 1, as defined by ORDER BY) to each row within the partition."
  ),
  supportsWindow = false,
  pivotResult = true
)
public class GenericUDAFRowNumber extends AbstractGenericUDAFResolver {

  static final Log LOG = LogFactory.getLog(GenericUDAFRowNumber.class.getName());

  @Override
  public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws SemanticException {
    if (parameters.length != 0) {
      throw new UDFArgumentTypeException(parameters.length - 1, "No argument is expected.");
    }
    return new GenericUDAFRowNumberEvaluator();
  }

  static class RowNumberBuffer implements AggregationBuffer {

    ArrayList<IntWritable> rowNums;
    int nextRow;

    void init() {
      rowNums = new ArrayList<IntWritable>();
    }

    RowNumberBuffer() {
      init();
      nextRow = 1;
    }

    void incr() {
      rowNums.add(new IntWritable(nextRow++));
    }
  }

  public static class GenericUDAFRowNumberEvaluator extends GenericUDAFEvaluator {

    @Override
    public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
      super.init(m, parameters);
      if (m != Mode.COMPLETE) {
        throw new HiveException("Only COMPLETE mode supported for row_number function");
      }

      return ObjectInspectorFactory.getStandardListObjectInspector(
        PrimitiveObjectInspectorFactory.writableIntObjectInspector);
    }

    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
      return new RowNumberBuffer();
    }

    @Override
    public void reset(AggregationBuffer agg) throws HiveException {
      ((RowNumberBuffer) agg).init();
    }

    @Override
    public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
      ((RowNumberBuffer) agg).incr();
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
      return ((RowNumberBuffer) agg).rowNums;
    }

  }
}

