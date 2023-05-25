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
import org.apache.hadoop.io.BytesWritable;
import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;

@Description(name = "bitmap_empty", value = "_FUNC_() - Return an empty bitmap.",
extended = "Example:\n >  select  _FUNC_()\n"
           + "binary"
)
public class UDFBitmapEmpty extends GenericUDF {
  public final static String FUNC_NAME = "bitmap_empty";
  private static final Logger LOG = LoggerFactory.getLogger(UDFBitmapEmpty.class.getName());


  @Override public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    checkArgsSize(arguments, 0, 0);
    return writableBinaryObjectInspector;
  }

  @Override public Object evaluate(DeferredObject[] arguments) throws HiveException {
    return new BytesWritable(RoaringBitmapSerDe.serialize(new RoaringBitmap()).array());
  }

  @Override public String getDisplayString(String[] children) {
    return getStandardDisplayString(FUNC_NAME, children, ",");
  }
  @Override public String getFuncName() {
    return FUNC_NAME;
  }
}