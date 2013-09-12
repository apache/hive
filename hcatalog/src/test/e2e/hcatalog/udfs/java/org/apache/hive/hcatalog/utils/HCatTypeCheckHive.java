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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hive.hcatalog.utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.ObjectInspectorOptions;

/**
 * A hive udf to check types of the fields read from hcat. A sample hive query which can use this is:
 *
 * create temporary function typecheck as 'org.apache.hive.hcatalog.utils.HCatTypeCheckHive';
 * select typecheck('map<string,string>+struct<num:int,str:string,dbl:double>+array<map<string,string>>+int', 
 * mymap, mytuple, bagofmap, rownum) from complex;
 *
 *
 * The first argument to the UDF is a string representing the schema of the columns in the table. 
 * The columns in the tables are the remaining args to it.
 * The schema specification consists of the types as given by "describe <table>"
 * with each column's type separated from the next column's type by a '+'
 *
 * The UDF will throw an exception (and cause the query to fail) if it does not
 * encounter the correct types.
 *
 * The output is a string representation of the data , type and hive category.
 * It is not advisable to use this against large dataset since the output would also
 * be large. 
 *
 */
public final class HCatTypeCheckHive extends GenericUDF {

  ObjectInspector[] argOIs;

  @Override
  public Object evaluate(DeferredObject[] args) throws HiveException {
    List<Object> row = new ArrayList<Object>();
    String typesStr = (String) getJavaObject(args[0].get(), argOIs[0], new ArrayList<Category>());
    String[] types = typesStr.split("\\+");
    for (int i = 0; i < types.length; i++) {
      types[i] = types[i].toLowerCase();
    }
    for (int i = 1; i < args.length; i++) {
      ObjectInspector oi = argOIs[i];
      List<ObjectInspector.Category> categories = new ArrayList<ObjectInspector.Category>();
      Object o = getJavaObject(args[i].get(), oi, categories);
      try {
        if (o != null) {
          Util.check(types[i - 1], o);
        }
      } catch (IOException e) {
        throw new HiveException(e);
      }
      row.add(o == null ? "null" : o);
      row.add(":" + (o == null ? "null" : o.getClass()) + ":" + categories);
    }
    return row.toString();
  }

  private Object getJavaObject(Object o, ObjectInspector oi, List<Category> categories) {
    if (categories != null) {
      categories.add(oi.getCategory());
    }
    if (oi.getCategory() == ObjectInspector.Category.LIST) {
      List<?> l = ((ListObjectInspector) oi).getList(o);
      List<Object> result = new ArrayList<Object>();
      ObjectInspector elemOI = ((ListObjectInspector) oi).getListElementObjectInspector();
      for (Object lo : l) {
        result.add(getJavaObject(lo, elemOI, categories));
      }
      return result;
    } else if (oi.getCategory() == ObjectInspector.Category.MAP) {
      Map<?, ?> m = ((MapObjectInspector) oi).getMap(o);
      Map<String, String> result = new HashMap<String, String>();
      ObjectInspector koi = ((MapObjectInspector) oi).getMapKeyObjectInspector();
      ObjectInspector voi = ((MapObjectInspector) oi).getMapValueObjectInspector();
      for (Entry<?, ?> e : m.entrySet()) {
        result.put((String) getJavaObject(e.getKey(), koi, null),
          (String) getJavaObject(e.getValue(), voi, null));
      }
      return result;

    } else if (oi.getCategory() == ObjectInspector.Category.STRUCT) {
      List<Object> s = ((StructObjectInspector) oi).getStructFieldsDataAsList(o);
      List<? extends StructField> sf = ((StructObjectInspector) oi).getAllStructFieldRefs();
      List<Object> result = new ArrayList<Object>();
      for (int i = 0; i < s.size(); i++) {
        result.add(getJavaObject(s.get(i), sf.get(i).getFieldObjectInspector(), categories));
      }
      return result;
    } else if (oi.getCategory() == ObjectInspector.Category.PRIMITIVE) {
      return ((PrimitiveObjectInspector) oi).getPrimitiveJavaObject(o);
    }
    throw new RuntimeException("Unexpected error!");
  }

  @Override
  public String getDisplayString(String[] arg0) {
    return null;
  }

  @Override
  public ObjectInspector initialize(ObjectInspector[] argOIs)
    throws UDFArgumentException {
    this.argOIs = argOIs;
    return ObjectInspectorFactory.getReflectionObjectInspector(String.class,
      ObjectInspectorOptions.JAVA);
  }

}
