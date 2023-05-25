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

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.util.bitmap.RoaringBitmapSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.io.BytesWritable;
import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;

@Description(name = "bitmap_from_string",
    value = "_FUNC_(x) - Convert a string into a bitmap. The input string should be a comma separated int (ranging from  Integer.MIN_VALUE to Integer.MAX_VALUE)"
,extended = "Example:\n"
    + "  > SELECT _FUNC_('1,2,3,4,5,6,7,8,9,10') FROM src LIMIT 1;\n"
    + "  binary"
)
public class UDFBitmapFromString extends GenericUDF implements Bitmap {

  public final static String FUNC_NAME = "bitmap_from_string";
  private static final Logger LOG = LoggerFactory.getLogger(UDFBitmapFromString.class.getName());
  private transient Converter[] converters = new Converter[1];
  private transient PrimitiveCategory[] inputTypes = new PrimitiveCategory[1];

  @Override public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    checkArgsSize(arguments, 1, 1);
    obtainStringConverter(arguments, 0, inputTypes, converters);
    return writableBinaryObjectInspector;
  }

  @Override public Object evaluate(DeferredObject[] arguments) throws HiveException {
    RoaringBitmap bitmap = new RoaringBitmap();
    String stringValue = getStringValue(arguments, 0, converters);
    if (StringUtils.isEmpty(stringValue)) return new BytesWritable(RoaringBitmapSerDe.serialize(bitmap).array());
    for (String s : stringValue.split(",")) {
      bitmap.add(checkBitIndex(s));
    }
    return new BytesWritable(RoaringBitmapSerDe.serialize(bitmap).array());
  }

  @Override public String getDisplayString(String[] children) {
    return getStandardDisplayString(FUNC_NAME, children);
  }
  @Override public String getFuncName() {
    return FUNC_NAME;
  }
}