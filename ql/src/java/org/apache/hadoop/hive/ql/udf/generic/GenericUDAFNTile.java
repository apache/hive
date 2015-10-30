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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.exec.WindowFunctionDescription;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AggregationBuffer;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.IntWritable;

@WindowFunctionDescription(
  description = @Description(
    name = "rank",
    value = "_FUNC_(x) NTILE allows easy calculation of tertiles, quartiles, deciles and other "
            +"common summary statistics. This function divides an ordered partition into a "
            + "specified number of groups called buckets and assigns a bucket number to each row "
            + "in the partition."
  ),
  supportsWindow = false,
  pivotResult = true
)
public class GenericUDAFNTile extends AbstractGenericUDAFResolver {

  static final Logger LOG = LoggerFactory.getLogger(GenericUDAFNTile.class.getName());

  @Override
  public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws SemanticException {
    if (parameters.length != 1) {
      throw new UDFArgumentTypeException(parameters.length - 1,
        "Exactly one argument is expected.");
    }
    ObjectInspector oi = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(parameters[0]);

    boolean c = ObjectInspectorUtils.compareTypes(oi,
      PrimitiveObjectInspectorFactory.writableIntObjectInspector);
    if (!c) {
      throw new UDFArgumentTypeException(0, "Number of tiles must be an int expression");
    }

    return new GenericUDAFNTileEvaluator();
  }

  static class NTileBuffer implements AggregationBuffer {

    Integer numBuckets;
    int numRows;

    void init() {
      numBuckets = null;
      numRows = 0;
    }

    NTileBuffer() {
      init();
    }
  }

  public static class GenericUDAFNTileEvaluator extends GenericUDAFEvaluator {

    private transient PrimitiveObjectInspector inputOI;

    @Override
    public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
      assert (parameters.length == 1);
      super.init(m, parameters);
      if (m != Mode.COMPLETE) {
        throw new HiveException("Only COMPLETE mode supported for NTile function");
      }
      inputOI = (PrimitiveObjectInspector) parameters[0];
      return ObjectInspectorFactory.getStandardListObjectInspector(
        PrimitiveObjectInspectorFactory.writableIntObjectInspector);
    }

    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
      return new NTileBuffer();
    }

    @Override
    public void reset(AggregationBuffer agg) throws HiveException {
      ((NTileBuffer) agg).init();
    }

    @Override
    public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
      NTileBuffer rb = (NTileBuffer) agg;
      if (rb.numBuckets == null) {
        rb.numBuckets = PrimitiveObjectInspectorUtils.getInt(parameters[0], inputOI);
      }
      rb.numRows++;
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
      NTileBuffer rb = (NTileBuffer) agg;
      ArrayList<IntWritable> res = new ArrayList<IntWritable>(rb.numRows);

      /*
       * if there is a remainder from numRows/numBuckets; then distribute increase the size of the first 'rem' buckets by 1.
       */

      int bucketsz = rb.numRows / rb.numBuckets;
      int rem = rb.numRows % rb.numBuckets;
      int start = 0;
      int bucket = 1;
      while (start < rb.numRows) {
        int end = start + bucketsz;
        if (rem > 0) {
          end++;
          rem--;
        }
        end = Math.min(rb.numRows, end);
        for (int i = start; i < end; i++) {
          res.add(new IntWritable(bucket));
        }
        start = end;
        bucket++;
      }

      return res;
    }

  }

}

