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
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFMurmurHash;
import org.apache.hadoop.hive.ql.util.bitmap.RoaringBitmapSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hive.common.util.Murmur3;
import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;

@Description(name = "bitmap_hash",
    value = "_FUNC_(x) - Calculating hash value for what your input and return a BITMAP which contain the hash value."
        ,extended = "Example:\n > select _FUNC_(\"i am cscs\");\n"
                    + "binary"
)
public class UDFBitmapHash extends GenericUDFMurmurHash {
  public final static String FUNC_NAME = "bitmap_hash";
  private static final Logger LOG = LoggerFactory.getLogger(UDFBitmapHash.class.getName());

  @Override public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentTypeException {
    super.initialize(arguments);
    return writableBinaryObjectInspector;
  }

  @Override public Object evaluate(DeferredObject[] arguments) throws HiveException {
    RoaringBitmap bitmap = new RoaringBitmap();
    bitmap.add(((IntWritable)super.evaluate(arguments)).get());
    return new BytesWritable(RoaringBitmapSerDe.serialize(bitmap).array());
  }

  @Override public String getDisplayString(String[] children) {
    return getStandardDisplayString(FUNC_NAME, children);
  }
  @Override public String getFuncName() {
    return FUNC_NAME;
  }
}