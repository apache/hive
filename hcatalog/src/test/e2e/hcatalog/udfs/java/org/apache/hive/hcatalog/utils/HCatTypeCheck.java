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
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.Utils;

/**
 * This UDF can be used to check that a tuple presented by HCatLoader has the
 * right types for the fields
 *
 * Usage is :
 *
 * register testudf.jar;
 * a = load 'numbers' using HCatLoader(...);
 * b = foreach a generate HCatTypeCheck('intnum1000:int,id:int,intnum5:int,intnum100:int,intnum:int,longnum:long,floatnum:float,doublenum:double', *);
 * store b into 'output';
 *
 * The schema string (the first argument to the UDF) is of the form one would provide in a 
 * pig load statement.
 *
 * The output should only contain the value '1' in all rows. (This UDF returns
 * the integer value 1 if all fields have the right type, else throws IOException)
 *
 */
public class HCatTypeCheck extends EvalFunc<Integer> {

  static HashMap<Byte, Class<?>> typeMap = new HashMap<Byte, Class<?>>();

  @Override
  public Integer exec(Tuple input) throws IOException {
    String schemaStr = (String) input.get(0);
    Schema s = null;
    try {
      s = getSchemaFromString(schemaStr);
    } catch (Exception e) {
      throw new IOException(e);
    }
    for (int i = 0; i < s.size(); i++) {
      check(s.getField(i).type, input.get(i + 1)); // input.get(i+1) since input.get(0) is the schema;
    }
    return 1;
  }

  static {
    typeMap.put(DataType.INTEGER, Integer.class);
    typeMap.put(DataType.LONG, Long.class);
    typeMap.put(DataType.FLOAT, Float.class);
    typeMap.put(DataType.DOUBLE, Double.class);
    typeMap.put(DataType.CHARARRAY, String.class);
    typeMap.put(DataType.TUPLE, Tuple.class);
    typeMap.put(DataType.MAP, Map.class);
    typeMap.put(DataType.BAG, DataBag.class);
  }


  private void die(String expectedType, Object o) throws IOException {
    throw new IOException("Expected " + expectedType + ", got " +
      o.getClass().getName());
  }


  private String check(Byte type, Object o) throws IOException {
    if (o == null) {
      return "";
    }
    if (check(typeMap.get(type), o)) {
      if (type.equals(DataType.MAP)) {
        Map<String, String> m = (Map<String, String>) o;
        check(m);
      } else if (type.equals(DataType.BAG)) {
        DataBag bg = (DataBag) o;
        for (Tuple tuple : bg) {
          Map<String, String> m = (Map<String, String>) tuple.get(0);
          check(m);
        }
      } else if (type.equals(DataType.TUPLE)) {
        Tuple t = (Tuple) o;
        if (!check(Integer.class, t.get(0)) ||
          !check(String.class, t.get(1)) ||
          !check(Double.class, t.get(2))) {
          die("t:tuple(num:int,str:string,dbl:double)", t);
        }
      }
    } else {
      die(typeMap.get(type).getName(), o);
    }
    return o.toString();
  }

  /**
   * @param m
   * @throws IOException
   */
  private void check(Map<String, String> m) throws IOException {
    for (Entry<String, String> e : m.entrySet()) {
      // just access key and value to ensure they are correct
      if (!check(String.class, e.getKey())) {
        die("String", e.getKey());
      }
      if (!check(String.class, e.getValue())) {
        die("String", e.getValue());
      }
    }

  }

  private boolean check(Class<?> expected, Object actual) {
    if (actual == null) {
      return true;
    }
    return expected.isAssignableFrom(actual.getClass());
  }

  Schema getSchemaFromString(String schemaString) throws Exception {
    /** ByteArrayInputStream stream = new ByteArrayInputStream(schemaString.getBytes()) ;
     QueryParser queryParser = new QueryParser(stream) ;
     Schema schema = queryParser.TupleSchema() ;
     Schema.setSchemaDefaultType(schema, org.apache.pig.data.DataType.BYTEARRAY);
     return schema;
     */
    return Utils.getSchemaFromString(schemaString);
  }

}
