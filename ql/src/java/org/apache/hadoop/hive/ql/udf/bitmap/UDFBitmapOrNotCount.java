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
import org.apache.hadoop.io.LongWritable;
import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.writableLongObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping.BINARY_GROUP;

@Description(name = "bitmap_not_count", value = "_FUNC_(x1,x2) - Calculate the set after x1 union new bitmap of  bitmap_range(0,bitmap_max(x2)) minus x2 and return the size of the result size.\n",

    extended = "Example:\n >  select  _FUNC_(bitmap_from_string('1,2,3'),bitmap_from_string('3,4,5'))\n" + "2")
public class UDFBitmapOrNotCount extends GenericUDF {
  public final static String FUNC_NAME = "bitmap_not_count";
  private static final Logger LOG = LoggerFactory.getLogger(UDFBitmapOrNotCount.class.getName());

  private transient ObjectInspectorConverters.Converter[] converters = null;
  private transient PrimitiveObjectInspector.PrimitiveCategory[] inputTypes = null;

  @Override public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    checkArgsSize(arguments, 2, Integer.MAX_VALUE);
    converters = new ObjectInspectorConverters.Converter[arguments.length];
    inputTypes = new PrimitiveObjectInspector.PrimitiveCategory[arguments.length];
    for (int i = 0; i < arguments.length; i++) {
      checkArgGroups(arguments, i, inputTypes, BINARY_GROUP);
      GenericUDFParamUtils.obtainBinaryConverter(arguments, i, inputTypes, converters);
    }
    return writableLongObjectInspector;
  }

  @Override public Object evaluate(DeferredObject[] arguments) throws HiveException {
    try {
      RoaringBitmap result = RoaringBitmapSerDe.deserialize(
          GenericUDFParamUtils.getBinaryValue(arguments, 0, converters).getBytes());
      long rangeEnd = 0;
      for (int i = 1; i < arguments.length; i++) {
        RoaringBitmap bitmap = RoaringBitmapSerDe.deserialize(GenericUDFParamUtils.getBinaryValue(arguments, i, converters).getBytes());
        if (bitmap.isEmpty()) continue;
        rangeEnd = bitmap.first()<0?bitmap.last():bitmap.select(bitmap.rank(Integer.MAX_VALUE) - 1);
        result.orNot(bitmap, rangeEnd+1);
      }
      return new LongWritable(result.getLongCardinality());
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