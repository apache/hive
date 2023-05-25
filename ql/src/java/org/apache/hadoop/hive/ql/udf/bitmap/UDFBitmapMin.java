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
package org.apache.hadoop.hive.ql.udf.bitmap;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFParamUtils;
import org.apache.hadoop.hive.ql.util.bitmap.RoaringBitmapSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.writableIntObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping.BINARY_GROUP;

@Description(name = "bitmap_min", value = "_FUNC_(x) - Calculate and return the max values of a bitmap.\n" ,
    extended = "Example:\n > select _FUNC_(bitmap_from_string('-2147483647,-2147483648,-1,1,2,3')) from src limit 1;\n" +
               "-2147483648")public class UDFBitmapMin extends GenericUDF {
  public final static String FUNC_NAME = "bitmap_min";
  private static final Logger LOG = LoggerFactory.getLogger(UDFBitmapMin.class.getName());
  private transient ObjectInspectorConverters.Converter[] converters = null;
  private transient PrimitiveObjectInspector.PrimitiveCategory[] inputTypes = null;

  @Override public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    checkArgsSize(arguments, 1, 1);
    converters = new ObjectInspectorConverters.Converter[arguments.length];
    inputTypes = new PrimitiveObjectInspector.PrimitiveCategory[arguments.length];
    for (int i = 0; i < arguments.length; i++) {
      checkArgGroups(arguments, i, inputTypes, BINARY_GROUP);
      GenericUDFParamUtils.obtainBinaryConverter(arguments, i, inputTypes, converters);
    }
    return writableIntObjectInspector;
  }

  @Override public Object evaluate(DeferredObject[] arguments) throws HiveException {
    try {
      RoaringBitmap bitmap = RoaringBitmapSerDe.deserialize(GenericUDFParamUtils.getBinaryValue(arguments, 0, converters).getBytes());
      if (bitmap.isEmpty()){
        return null;
      }else{
        return bitmap.last()<0?bitmap.select(bitmap.rank(Integer.MAX_VALUE)):bitmap.first();
      }
    } catch (IOException e) {
      throw new HiveException(e);
    }
  }

  @Override public String getDisplayString(String[] children) {
    return getStandardDisplayString(FUNC_NAME, children, ",");
  }
  @Override public String getFuncName() {
    return FUNC_NAME;
  }
}