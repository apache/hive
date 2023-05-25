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
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFUtils;
import org.apache.hadoop.hive.ql.util.bitmap.RoaringBitmapSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;

import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaIntObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping.BINARY_GROUP;

@Description(name = "bitmap_to_array", value = "_FUNC_(x) - Convert a input BITMAP to Array.", extended = "Example:\n > select _FUNC_(bitmap_from_string('1,2,3'));\n" + "[1,2,3]")
public class UDFBitmapToArray extends GenericUDF {
  public final static String FUNC_NAME = "bitmap_to_array";
  private static final Logger LOG = LoggerFactory.getLogger(UDFBitmapToArray.class.getName());
  private transient ObjectInspectorConverters.Converter[] converters = new ObjectInspectorConverters.Converter[1];
  private transient PrimitiveObjectInspector.PrimitiveCategory[] inputTypes = new PrimitiveObjectInspector.PrimitiveCategory[1];

  @Override public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {

    checkArgsSize(arguments, 1, 1);

    checkArgGroups(arguments, 0, inputTypes, BINARY_GROUP);
    GenericUDFParamUtils.obtainBinaryConverter(arguments, 0, inputTypes, converters);

    GenericUDFUtils.ReturnObjectInspectorResolver returnOIResolver = new GenericUDFUtils.ReturnObjectInspectorResolver(true);
    ObjectInspector returnOI = returnOIResolver.get(javaIntObjectInspector);
    return ObjectInspectorFactory.getStandardListObjectInspector(returnOI);
  }

  @Override public Object evaluate(DeferredObject[] arguments) throws HiveException {
    try {
      return RoaringBitmapSerDe.deserialize(GenericUDFParamUtils.getBinaryValue(arguments, 0, converters).getBytes()).toArray();
    } catch (IOException e) {
      throw new HiveException(e);
    }

  }

  @Override public String getDisplayString(String[] children) {
    return getStandardDisplayString(FUNC_NAME, children);
  }
  @Override public String getFuncName() {
    return FUNC_NAME;
  }
}