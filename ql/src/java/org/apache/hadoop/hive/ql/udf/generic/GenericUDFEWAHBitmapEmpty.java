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

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.index.bitmap.BitmapObjectInput;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.LongWritable;

@Description(name = "ewah_bitmap_empty", value = "_FUNC_(bitmap) - "
    + "Predicate that tests whether an EWAH-compressed bitmap is all zeros ")
public class GenericUDFEWAHBitmapEmpty extends GenericUDF {
  ObjectInspector bitmapOI;
  BooleanObjectInspector boolOI;

@Override
public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
  if (arguments.length != 1) {
    throw new UDFArgumentLengthException(
      "The function EWAH_BITMAP_EMPTY(b) takes exactly 1 argument");
  }

  if (arguments[0].getCategory().equals(Category.LIST)) {
    bitmapOI = (ListObjectInspector) arguments[0];
  } else {
      throw new UDFArgumentTypeException(0, "\""
        + Category.LIST.toString().toLowerCase()
        + "\" is expected at function EWAH_BITMAP_EMPTY, but \""
        + arguments[0].getTypeName() + "\" is found");
  }

  boolOI = PrimitiveObjectInspectorFactory.writableBooleanObjectInspector;
  return boolOI;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    assert (arguments.length == 1);
    Object b = arguments[0].get();

    ListObjectInspector lloi = (ListObjectInspector) bitmapOI;
    int length = lloi.getListLength(b);
    ArrayList<LongWritable> bitmapArray = new ArrayList<LongWritable>();
    for (int i = 0; i < length; i++) {
      long l = PrimitiveObjectInspectorUtils.getLong(
          lloi.getListElement(b, i),
          (PrimitiveObjectInspector) lloi.getListElementObjectInspector());
      bitmapArray.add(new LongWritable(l));
    }

    BitmapObjectInput bitmapObjIn = new BitmapObjectInput(bitmapArray);
    EWAHCompressedBitmap bitmap = new EWAHCompressedBitmap();
    try {
      bitmap.readExternal(bitmapObjIn);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

     // Add return true only if bitmap is all zeros.
     return new BooleanWritable(!bitmap.iterator().hasNext());
  }


  @Override
  public String getDisplayString(String[] children) {
    StringBuilder sb = new StringBuilder();
    sb.append("EWAH_BITMAP_EMPTY(");
    for (int i = 0; i < children.length; i++) {
      sb.append(children[i]);
      if (i + 1 != children.length) {
        sb.append(",");
      }
    }
    sb.append(")");
    return sb.toString();
  }
}
