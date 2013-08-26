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

import java.io.IOException;
import java.util.ArrayList;

import javaewah.EWAHCompressedBitmap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.index.bitmap.BitmapObjectInput;
import org.apache.hadoop.hive.ql.index.bitmap.BitmapObjectOutput;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.util.StringUtils;


/**
 * GenericUDAFEWAHBitmap.
 *
 */
@Description(name = "ewah_bitmap", value = "_FUNC_(expr) - Returns an EWAH-compressed bitmap representation of a column.")
public class GenericUDAFEWAHBitmap extends AbstractGenericUDAFResolver {

  static final Log LOG = LogFactory.getLog(GenericUDAFEWAHBitmap.class.getName());

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
    return new GenericUDAFEWAHBitmapEvaluator();
  }

  //The UDAF evaluator assumes that all rows it's evaluating have
  //the same (desired) value.
  public static class GenericUDAFEWAHBitmapEvaluator extends GenericUDAFEvaluator {

    // For PARTIAL1 and COMPLETE: ObjectInspectors for original data
    private PrimitiveObjectInspector inputOI;

    // For PARTIAL2 and FINAL: ObjectInspectors for partial aggregations
    // (lists of bitmaps)
    private transient StandardListObjectInspector loi;
    private transient StandardListObjectInspector internalMergeOI;

    @Override
    public ObjectInspector init(Mode m, ObjectInspector[] parameters)
        throws HiveException {
      super.init(m, parameters);
      // init output object inspectors
      // The output of a partial aggregation is a list
      if (m == Mode.PARTIAL1) {
        inputOI = (PrimitiveObjectInspector) parameters[0];
        return ObjectInspectorFactory
            .getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableLongObjectInspector);
      } else if (m == Mode.PARTIAL2 || m == Mode.FINAL) {
        internalMergeOI = (StandardListObjectInspector) parameters[0];
        inputOI = PrimitiveObjectInspectorFactory.writableByteObjectInspector;
        loi = (StandardListObjectInspector) ObjectInspectorFactory
            .getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableLongObjectInspector);
        return loi;
      } else { // Mode.COMPLETE, ie. no map-side aggregation, requires ordering
        inputOI = PrimitiveObjectInspectorFactory.writableByteObjectInspector;
        loi = (StandardListObjectInspector) ObjectInspectorFactory
            .getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableLongObjectInspector);
        return loi;
      }
    }

    /** class for storing the current partial result aggregation */
    @AggregationType(estimable = true)
    static class BitmapAgg extends AbstractAggregationBuffer {
      EWAHCompressedBitmap bitmap;
      @Override
      public int estimate() {
        return bitmap.sizeInBytes();
      }
    }

    @Override
    public void reset(AggregationBuffer agg) throws HiveException {

        ((BitmapAgg) agg).bitmap = new EWAHCompressedBitmap();
    }

    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
      BitmapAgg result = new BitmapAgg();
      reset(result);
      return result;
    }

    @Override
    public void iterate(AggregationBuffer agg, Object[] parameters)
        throws HiveException {
        assert (parameters.length == 1);
        Object p = parameters[0];
        if (p != null) {
            BitmapAgg myagg = (BitmapAgg) agg;
            try {
                int row = PrimitiveObjectInspectorUtils.getInt(p, inputOI);
                addBitmap(row, myagg);
            } catch (NumberFormatException e) {
                LOG.warn(getClass().getSimpleName() + " " +
                        StringUtils.stringifyException(e));
            }
        }
    }


    @Override
    public Object terminate(AggregationBuffer agg) throws HiveException {
        BitmapAgg myagg = (BitmapAgg) agg;

        BitmapObjectOutput bitmapObjOut = new BitmapObjectOutput();
        try {
          myagg.bitmap.writeExternal(bitmapObjOut);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
        return bitmapObjOut.list();
    }

    @Override
    public void merge(AggregationBuffer agg, Object partial)
        throws HiveException {
      BitmapAgg myagg = (BitmapAgg) agg;
      ArrayList<LongWritable> partialResult = (ArrayList<LongWritable>) internalMergeOI.getList(partial);
      BitmapObjectInput bitmapObjIn = new BitmapObjectInput(partialResult);
      EWAHCompressedBitmap partialBitmap = new EWAHCompressedBitmap();
      try {
        partialBitmap.readExternal(bitmapObjIn);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      myagg.bitmap = myagg.bitmap.or(partialBitmap);
    }

    @Override
    public Object terminatePartial(AggregationBuffer agg) throws HiveException {
      BitmapAgg myagg = (BitmapAgg) agg;
      BitmapObjectOutput bitmapObjOut = new BitmapObjectOutput();
      try {
        myagg.bitmap.writeExternal(bitmapObjOut);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      return bitmapObjOut.list();
    }

    private void addBitmap(int newRow, BitmapAgg myagg) {
        if (!myagg.bitmap.set(newRow)) {
          throw new RuntimeException("Can't set bits out of order with EWAHCompressedBitmap");
        }
    }
  }
}
