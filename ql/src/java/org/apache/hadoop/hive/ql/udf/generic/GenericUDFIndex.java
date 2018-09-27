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

package org.apache.hadoop.hive.ql.udf.generic;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedExpressions;
import org.apache.hadoop.hive.ql.exec.vector.expressions.ListIndexColColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.ListIndexColScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorUDFMapIndexDecimalCol;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorUDFMapIndexDecimalScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorUDFMapIndexDoubleCol;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorUDFMapIndexDoubleScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorUDFMapIndexLongCol;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorUDFMapIndexLongScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorUDFMapIndexStringCol;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorUDFMapIndexStringScalar;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.IntWritable;

/**
 * GenericUDFIndex.
 *
 */
@Description(name = "index", value = "_FUNC_(a, n) - Returns the n-th element of a ")
@VectorizedExpressions({ListIndexColScalar.class, ListIndexColColumn.class,
    VectorUDFMapIndexStringScalar.class, VectorUDFMapIndexLongScalar.class,
    VectorUDFMapIndexDoubleScalar.class, VectorUDFMapIndexDecimalScalar.class,
    VectorUDFMapIndexStringCol.class, VectorUDFMapIndexLongCol.class,
    VectorUDFMapIndexDoubleCol.class, VectorUDFMapIndexDecimalCol.class})
public class GenericUDFIndex extends GenericUDF {

  private transient MapObjectInspector mapOI;
  private transient ListObjectInspector listOI;
  private transient ObjectInspectorConverters.Converter converter;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length != 2) {
      throw new UDFArgumentLengthException(
          "The function INDEX accepts exactly 2 arguments.");
    }

    if (arguments[0] instanceof MapObjectInspector) {
      // index into a map
      mapOI = (MapObjectInspector) arguments[0];
      listOI = null;
    } else if (arguments[0] instanceof ListObjectInspector) {
      // index into a list
      listOI = (ListObjectInspector) arguments[0];
      mapOI = null;
    } else {
      throw new UDFArgumentTypeException(0, "\""
          + Category.MAP.toString().toLowerCase() + "\" or \""
          + Category.LIST.toString().toLowerCase()
          + "\" is expected at function INDEX, but \""
          + arguments[0].getTypeName() + "\" is found");
    }

    // index has to be a primitive
    if (!(arguments[1] instanceof PrimitiveObjectInspector)) {
      throw new UDFArgumentTypeException(1, "Primitive Type is expected but "
          + arguments[1].getTypeName() + "\" is found");
    }
    PrimitiveObjectInspector inputOI = (PrimitiveObjectInspector) arguments[1];
    ObjectInspector returnOI;
    ObjectInspector indexOI;
    if (mapOI != null) {
      indexOI = ObjectInspectorConverters.getConvertedOI(
          inputOI, mapOI.getMapKeyObjectInspector());
      returnOI = mapOI.getMapValueObjectInspector();
    } else {
      indexOI = PrimitiveObjectInspectorFactory.writableIntObjectInspector;
      returnOI = listOI.getListElementObjectInspector();
    }
    converter = ObjectInspectorConverters.getConverter(inputOI, indexOI);

    return returnOI;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    assert (arguments.length == 2);
    Object index = arguments[1].get();

    Object indexObject = converter.convert(index);
    if (indexObject == null) {
      return null;
    }
    if (mapOI != null) {
      return mapOI.getMapValueElement(arguments[0].get(), indexObject);
    }
    return listOI.getListElement(arguments[0].get(), ((IntWritable)indexObject).get());
  }

  @Override
  public String getDisplayString(String[] children) {
    assert (children.length == 2);
    return children[0] + "[" + children[1] + "]";
  }
}
