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
import org.apache.hadoop.hive.ql.plan.ptf.WindowFrameDef;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AggregationBuffer;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFRank.GenericUDAFAbstractRankEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFRank.RankBuffer;
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
    boolean supportsStreaming;

    void init() {
      rowNums = new ArrayList<IntWritable>();
      nextRow = 1;
      if (supportsStreaming) {
        rowNums.add(null);
      }
    }

    RowNumberBuffer(boolean supportsStreaming) {
      this.supportsStreaming = supportsStreaming;
      init();
    }

    void incr() {
      if (supportsStreaming) {
        rowNums.set(0,new IntWritable(nextRow++));
      } else {
        rowNums.add(new IntWritable(nextRow++));
      }
    }
  }

  public static class GenericUDAFAbstractRowNumberEvaluator extends GenericUDAFEvaluator {
    boolean isStreamingMode = false;

    protected boolean isStreaming() {
      return isStreamingMode;
    }

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
      return new RowNumberBuffer(isStreamingMode);
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

  public static class GenericUDAFRowNumberEvaluator extends GenericUDAFAbstractRowNumberEvaluator
  implements ISupportStreamingModeForWindowing {

    @Override
    public Object getNextResult(AggregationBuffer agg) throws HiveException {
      return ((RowNumberBuffer) agg).rowNums.get(0);
    }

    @Override
    public GenericUDAFEvaluator getWindowingEvaluator(WindowFrameDef wFrmDef) {
      isStreamingMode = true;
      return this;
    }

    @Override
    public int getRowsRemainingAfterTerminate() throws HiveException {
      return 0;
    }
  }

}

