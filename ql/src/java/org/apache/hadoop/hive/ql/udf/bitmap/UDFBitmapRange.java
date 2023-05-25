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
import org.apache.hadoop.hive.ql.util.bitmap.RoaringBitmapSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.io.BytesWritable;
import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;

@Description(name = "bitmap_range", value = "_FUNC_(rangeStart,rangeEnd) -  return the bitmap all integers in [rangeStart,rangeEnd).\n", extended = "Example:\n >  select  _FUNC_(1,100) \n" + "binary")
public class UDFBitmapRange extends GenericUDF implements Bitmap{
  public final static String FUNC_NAME = "bitmap_range";
  private static final Logger LOG = LoggerFactory.getLogger(UDFBitmapRange.class.getName());

  private transient ObjectInspectorConverters.Converter[] converters = null;
  private transient PrimitiveObjectInspector.PrimitiveCategory[] inputTypes = null;

  @Override public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    checkArgsSize(arguments, 2, 2);
    converters = new ObjectInspectorConverters.Converter[arguments.length];
    inputTypes = new PrimitiveObjectInspector.PrimitiveCategory[arguments.length];
    for (int i = 0; i < arguments.length; i++) {
      obtainLongConverter(arguments, i, inputTypes, converters);
    }
    return writableBinaryObjectInspector;
  }

  @Override public Object evaluate(DeferredObject[] arguments) throws HiveException {
    RoaringBitmap result = new RoaringBitmap();
    Long start = getLongValue(arguments, 0, converters);
    Long end   = getLongValue(arguments, 1, converters);
    checkBitRange(start, end);
    result.add(start,end);
    return new BytesWritable(RoaringBitmapSerDe.serialize(result).array());
  }

  @Override public String getDisplayString(String[] children) {
    return getStandardDisplayString(FUNC_NAME, children, ",");
  }

  @Override public String getFuncName() {
    return FUNC_NAME;
  }
}