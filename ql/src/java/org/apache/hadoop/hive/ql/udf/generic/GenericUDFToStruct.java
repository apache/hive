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
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.SettableUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.VoidObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import java.util.ArrayList;
import java.util.List;

/**
 * GenericUDFMap.
 *
 */
@Description(name = "toStruct", value = "_FUNC_(key0, value0, key1, value1...) - "
    + "Creates a map with the given key/value pairs ")
public class GenericUDFToStruct extends GenericUDF implements SettableUDF {
  private transient Converter[] converters;
  private StructTypeInfo typeInfo;


  // Must be deterministic order map for consistent q-test output across Java versions - see HIVE-9161
  List<Object> ret = new ArrayList<>();

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {

    return typeInfo.createObjectInspector();

//    GenericUDFUtils.ReturnObjectInspectorResolver keyOIResolver =
//        new GenericUDFUtils.ReturnObjectInspectorResolver(true);
//    GenericUDFUtils.ReturnObjectInspectorResolver valueOIResolver =
//        new GenericUDFUtils.ReturnObjectInspectorResolver(true);
//
//    for (int i = 0; i < arguments.length; i++) {
//      if (i % 2 == 0) {
//        // Keys
//        if (!(arguments[i] instanceof PrimitiveObjectInspector)) {
//          throw new UDFArgumentTypeException(1,
//              "Primitive Type is expected but " + arguments[i].getTypeName()
//              + "\" is found");
//        }
//        if (!keyOIResolver.update(arguments[i])) {
//          throw new UDFArgumentTypeException(i, "Key type \""
//              + arguments[i].getTypeName()
//              + "\" is different from preceding key types. "
//              + "Previous key type was \"" + arguments[i - 2].getTypeName()
//              + "\"");
//        }
//      } else {
//        // Values
//        if (!valueOIResolver.update(arguments[i]) && !compatibleTypes(arguments[i], arguments[i-2])) {
//          throw new UDFArgumentTypeException(i, "Value type \""
//              + arguments[i].getTypeName()
//              + "\" is different from preceding value types. "
//              + "Previous value type was \"" + arguments[i - 2].getTypeName()
//              + "\"");
//        }
//      }
//    }
//
//    ObjectInspector keyOI =
//        keyOIResolver.get(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
//    ObjectInspector valueOI =
//        valueOIResolver.get(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
//
//    converters = new Converter[arguments.length];
//
//    for (int i = 0; i < arguments.length; i++) {
//      converters[i] = ObjectInspectorConverters.getConverter(arguments[i],
//          i % 2 == 0 ? keyOI : valueOI);
//    }
//
//    return ObjectInspectorFactory.getStandardMapObjectInspector(keyOI, valueOI);
  }

  private boolean compatibleTypes(ObjectInspector current, ObjectInspector prev) {

    if (current instanceof VoidObjectInspector || prev instanceof VoidObjectInspector) {
      // we allow null values for map.
      return true;
    }
    if (current instanceof ListObjectInspector && prev instanceof ListObjectInspector && (
      ((ListObjectInspector)current).getListElementObjectInspector() instanceof VoidObjectInspector ||
      ((ListObjectInspector)prev).getListElementObjectInspector() instanceof VoidObjectInspector)) {
      // array<null> is compatible with any other array<type>
      return true;
    }
    return false;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    if (arguments.length == 0 || arguments[0] == null || arguments[0].get() == null) {
      return null;
    }

    ret.clear();
    return ret;
  }

  @Override
  public String getDisplayString(String[] children) {
    StringBuilder sb = new StringBuilder();
    sb.append("toArray(");
    for (int i = 0; i < children.length; ++i) {
      if (i != 0) {
        sb.append(",");
      }
      sb.append(children[i]);
    }
    sb.append(")");
    return sb.toString();
  }

  @Override
  public void setTypeInfo(TypeInfo typeInfo) throws UDFArgumentException {
    this.typeInfo = (StructTypeInfo) typeInfo;
  }

  @Override
  public TypeInfo getTypeInfo() {
    return typeInfo;
  }
}
