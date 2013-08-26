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
import java.util.List;

import javaewah.EWAHCompressedBitmap;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.index.bitmap.BitmapObjectInput;
import org.apache.hadoop.hive.ql.index.bitmap.BitmapObjectOutput;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.io.LongWritable;

/**
 * An abstract class for a UDF that performs a binary operation between two EWAH-compressed bitmaps.
 * For example: Bitmap OR and AND operations between two EWAH-compressed bitmaps.
 */
abstract public class AbstractGenericUDFEWAHBitmapBop extends GenericUDF {
  protected final ArrayList<Object> ret = new ArrayList<Object>();
  private transient ObjectInspector b1OI;
  private final String name;

  AbstractGenericUDFEWAHBitmapBop(String name) {
    this.name = name;
  }

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length != 2) {
      throw new UDFArgumentLengthException(
        "The function " + name + "(b1, b2) takes exactly 2 arguments");
    }

    if (arguments[0].getCategory().equals(Category.LIST)) {
      b1OI = (ListObjectInspector) arguments[0];
    } else {
        throw new UDFArgumentTypeException(0, "\""
          + Category.LIST.toString().toLowerCase()
          + "\" is expected at function " + name + ", but \""
          + arguments[0].getTypeName() + "\" is found");
    }

    if (!arguments[1].getCategory().equals(Category.LIST)) {
        throw new UDFArgumentTypeException(1, "\""
          + Category.LIST.toString().toLowerCase()
          + "\" is expected at function " + name + ", but \""
          + arguments[1].getTypeName() + "\" is found");

    }
    return ObjectInspectorFactory
        .getStandardListObjectInspector(PrimitiveObjectInspectorFactory
            .writableLongObjectInspector);
  }

  protected abstract EWAHCompressedBitmap bitmapBop(
    EWAHCompressedBitmap bitmap1, EWAHCompressedBitmap bitmap2);

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    assert (arguments.length == 2);
    Object b1 = arguments[0].get();
    Object b2 = arguments[1].get();

    EWAHCompressedBitmap bitmap1 = wordArrayToBitmap(b1);
    EWAHCompressedBitmap bitmap2 = wordArrayToBitmap(b2);

    EWAHCompressedBitmap bitmapAnd = bitmapBop(bitmap1, bitmap2);

    BitmapObjectOutput bitmapObjOut = new BitmapObjectOutput();
    try {
      bitmapAnd.writeExternal(bitmapObjOut);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    ret.clear();
    List<LongWritable> retList = bitmapToWordArray(bitmapAnd);
    for (LongWritable l : retList) {
      ret.add(l);
    }
    return ret;
  }
  
  protected EWAHCompressedBitmap wordArrayToBitmap(Object b) {
    ListObjectInspector lloi = (ListObjectInspector) b1OI;
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
    return bitmap;
  }

  protected List<LongWritable> bitmapToWordArray(EWAHCompressedBitmap bitmap) {
    BitmapObjectOutput bitmapObjOut = new BitmapObjectOutput();
    try {
      bitmap.writeExternal(bitmapObjOut);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return bitmapObjOut.list();
  }
  
  @Override
  public String getDisplayString(String[] children) {
    StringBuilder sb = new StringBuilder();
    sb.append(name);
    sb.append("(");
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
