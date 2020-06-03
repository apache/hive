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

import org.apache.hadoop.hive.common.ndv.NumDistinctValueEstimator;
import org.apache.hadoop.hive.common.ndv.NumDistinctValueEstimatorFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.LongWritable;


/**
 * GenericUDFNDVComputeBitVector. The ndv_compute_bit_vector function can be used on top of
 * compute_bit_vector aggregate function to extract an estimate of the ndv from it.
 */
@Description(name = "ndv_compute_bit_vector",
    value = "_FUNC_(x) - Extracts NDV from bit vector.")
public class GenericUDFNDVComputeBitVector extends GenericUDF {

  protected transient BinaryObjectInspector inputOI;
  protected final LongWritable result = new LongWritable(0);

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments[0].getCategory() != Category.PRIMITIVE) {
      throw new UDFArgumentTypeException(0,
          "ndv_compute_bitvector input only takes primitive types, got " + arguments[0].getTypeName());
    }
    PrimitiveObjectInspector objectInspector = (PrimitiveObjectInspector) arguments[0];
    if (objectInspector.getPrimitiveCategory() != PrimitiveCategory.BINARY) {
      throw new UDFArgumentTypeException(0,
          "ndv_compute_bitvector input only takes BINARY type, got " + arguments[0].getTypeName());
    }
    inputOI = (BinaryObjectInspector) arguments[0];
    return PrimitiveObjectInspectorFactory.writableLongObjectInspector;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    if (arguments[0] == null) {
      return null;
    }
    Object input = arguments[0].get();
    if (input == null) {
      return null;
    }

    byte[] buf = inputOI.getPrimitiveJavaObject(input);
    if (buf == null || buf.length == 0) {
      return null;
    }
    NumDistinctValueEstimator numDV =
        NumDistinctValueEstimatorFactory.getNumDistinctValueEstimator(buf);
    result.set(numDV.estimateNumDistinctValues());
    return result;
  }

  @Override
  public String getDisplayString(String[] children) {
    return getStandardDisplayString("ndv_compute_bit_vector", children, ",");
  }
}
