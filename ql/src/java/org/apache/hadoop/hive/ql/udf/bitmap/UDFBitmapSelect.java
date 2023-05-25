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
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.writableIntObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping.BINARY_GROUP;

@Description(name = "bitmap_select", value = "_FUNC_(bitmap,int) - Return the jth value stored in this bitmap. The provided value needs to be smaller than the cardinality otherwise an IllegalArgumentException exception is thrown. The smallest value is at index 0. \n", extended = "Example:\n >  select  _FUNC_(bitmap(1,2,3,5),3) \n" + "5")
public class UDFBitmapSelect extends GenericUDF {
  public final static String FUNC_NAME = "bitmap_select";
  private static final Logger LOG = LoggerFactory.getLogger(UDFBitmapRank.class.getName());

  private transient ObjectInspectorConverters.Converter[] converters = null;
  private transient PrimitiveObjectInspector.PrimitiveCategory[] inputTypes = null;

  @Override public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    checkArgsSize(arguments, 2, 2);
    converters = new ObjectInspectorConverters.Converter[arguments.length];
    inputTypes = new PrimitiveObjectInspector.PrimitiveCategory[arguments.length];
    checkArgGroups(arguments, 0, inputTypes, BINARY_GROUP);
    GenericUDFParamUtils.obtainBinaryConverter(arguments, 0, inputTypes, converters);
    for (int i = 1; i < arguments.length; i++) {
      obtainIntConverter(arguments, i, inputTypes, converters);
    }
    return writableIntObjectInspector;
  }

  @Override public Object evaluate(DeferredObject[] arguments) throws HiveException {
    try {
      RoaringBitmap result = RoaringBitmapSerDe.deserialize(GenericUDFParamUtils.getBinaryValue(arguments, 0, converters).getBytes());
      Integer selectIndex = getIntValue(arguments, 1, converters);
      if (result.getCardinality() < selectIndex) {
        return null;
      }
      return new IntWritable(result.select(selectIndex));
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