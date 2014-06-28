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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.util.JavaDataModel;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.LongWritable;

/**
 * This class implements the COUNT aggregation function as in SQL.
 */
@Description(name = "count",
    value = "_FUNC_(*) - Returns the total number of retrieved rows, including "
          +        "rows containing NULL values.\n"

          + "_FUNC_(expr) - Returns the number of rows for which the supplied "
          +        "expression is non-NULL.\n"

          + "_FUNC_(DISTINCT expr[, expr...]) - Returns the number of rows for "
          +        "which the supplied expression(s) are unique and non-NULL.")
public class GenericUDAFCount implements GenericUDAFResolver2 {

  private static final Log LOG = LogFactory.getLog(GenericUDAFCount.class.getName());

  @Override
  public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters)
      throws SemanticException {
    // This method implementation is preserved for backward compatibility.
    return new GenericUDAFCountEvaluator();
  }

  @Override
  public GenericUDAFEvaluator getEvaluator(GenericUDAFParameterInfo paramInfo)
  throws SemanticException {

    TypeInfo[] parameters = paramInfo.getParameters();

    if (parameters.length == 0) {
      if (!paramInfo.isAllColumns()) {
        throw new UDFArgumentException("Argument expected");
      }
      assert !paramInfo.isDistinct() : "DISTINCT not supported with *";
    } else {
      if (parameters.length > 1 && !paramInfo.isDistinct()) {
        throw new UDFArgumentException("DISTINCT keyword must be specified");
      }
      assert !paramInfo.isAllColumns() : "* not supported in expression list";
    }

    return new GenericUDAFCountEvaluator().setCountAllColumns(
        paramInfo.isAllColumns());
  }

  /**
   * GenericUDAFCountEvaluator.
   *
   */
  public static class GenericUDAFCountEvaluator extends GenericUDAFEvaluator {
    private boolean countAllColumns = false;
    private LongObjectInspector partialCountAggOI;
    private LongWritable result;

    @Override
    public ObjectInspector init(Mode m, ObjectInspector[] parameters)
    throws HiveException {
      super.init(m, parameters);
      partialCountAggOI =
        PrimitiveObjectInspectorFactory.writableLongObjectInspector;
      result = new LongWritable(0);
      return PrimitiveObjectInspectorFactory.writableLongObjectInspector;
    }

    private GenericUDAFCountEvaluator setCountAllColumns(boolean countAllCols) {
      countAllColumns = countAllCols;
      return this;
    }

    /** class for storing count value. */
    @AggregationType(estimable = true)
    static class CountAgg extends AbstractAggregationBuffer {
      long value;
      @Override
      public int estimate() { return JavaDataModel.PRIMITIVES2; }
    }

    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
      CountAgg buffer = new CountAgg();
      reset(buffer);
      return buffer;
    }

    @Override
    public void reset(AggregationBuffer agg) throws HiveException {
      ((CountAgg) agg).value = 0;
    }

    @Override
    public void iterate(AggregationBuffer agg, Object[] parameters)
      throws HiveException {
      // parameters == null means the input table/split is empty
      if (parameters == null) {
        return;
      }
      if (countAllColumns) {
        assert parameters.length == 0;
        ((CountAgg) agg).value++;
      } else {
        boolean countThisRow = true;
        for (Object nextParam : parameters) {
          if (nextParam == null) {
            countThisRow = false;
            break;
          }
        }
        if (countThisRow) {
          ((CountAgg) agg).value++;
        }
      }
    }

    @Override
    public void merge(AggregationBuffer agg, Object partial)
      throws HiveException {
      if (partial != null) {
        long p = partialCountAggOI.get(partial);
        ((CountAgg) agg).value += p;
      }
    }

    @Override
    public Object terminate(AggregationBuffer agg) throws HiveException {
      result.set(((CountAgg) agg).value);
      return result;
    }

    @Override
    public Object terminatePartial(AggregationBuffer agg) throws HiveException {
      return terminate(agg);
    }
  }
}
