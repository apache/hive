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
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * UDFSplitMapPrivs.
 *
 */

@Description(name = "split_map_privs", value = "_FUNC_(str, regex) - Splits binary str and maps to privilege type "
    + "regex", extended = "Example:\n" + "  > SELECT _FUNC_('0 1 1 0 1 1 0 0 0', ' ') FROM src LIMIT 1;\n"
    + "  [\"UPDATE\", \"CREATE\", \"ALTER\", \"INDEX\"]") class PrivilegeMap {
  private Map<Integer, String> privilegeMap = new HashMap<Integer, String>();

  Map<Integer, String> getPrivilegeMap() {

    privilegeMap.put(0, "SELECT");
    privilegeMap.put(1, "UPDATE");
    privilegeMap.put(2, "CREATE");
    privilegeMap.put(3, "DROP");
    privilegeMap.put(4, "ALTER");
    privilegeMap.put(5, "INDEX");
    privilegeMap.put(6, "LOCK");
    privilegeMap.put(7, "READ");
    privilegeMap.put(8, "WRITE");
    privilegeMap.put(9, "ALL");

    return privilegeMap;
  }
}

/**
 * UDFSplitMapPrivs.
 * "_FUNC_(str, regex) - Splits binary str and maps to privilege type "
 *      "Example: > SELECT _FUNC_('0 1 1 0 1 1 0 0 0', ' ') FROM src LIMIT 1;"
 *     output: "  ["UPDATE", "CREATE", "ALTER", "INDEX"]"
 */
public class GenericUDFStringToPrivilege extends GenericUDF {
  private transient ObjectInspectorConverters.Converter[] converters;
  private transient Pattern constPattern;

  private PrivilegeMap privsMap = new PrivilegeMap();

  @Override public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length != 2) {
      throw new UDFArgumentLengthException("The function split_map_privs(s, ' ') takes exactly 2 arguments.");
    }

    converters = new ObjectInspectorConverters.Converter[arguments.length];
    for (int i = 0; i < arguments.length; i++) {
      converters[i] = ObjectInspectorConverters
          .getConverter(arguments[i], PrimitiveObjectInspectorFactory.writableStringObjectInspector);
    }

    ObjectInspector rightArg = arguments[1];
    if (rightArg instanceof ConstantObjectInspector) {
      constPattern = Pattern.compile(((ConstantObjectInspector) rightArg).
          getWritableConstantValue().toString());
    }

    return ObjectInspectorFactory
        .getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableStringObjectInspector);
  }

  @Override public Object evaluate(DeferredObject[] arguments) throws HiveException {
    assert (arguments.length == 2);

    if (arguments[0].get() == null || arguments[1].get() == null) {
      return null;
    }

    Text s = (Text) converters[0].convert(arguments[0].get());
    ArrayList<Text> result = new ArrayList<Text>();
    int index = 0;
    Map<Integer, String> privs = privsMap.getPrivilegeMap();

    if (constPattern == null) {
      Text regex = (Text) converters[1].convert(arguments[1].get());
      for (String str : s.toString().split(regex.toString(), -1)) {
        if ("1".equals(str)) {
          result.add(new Text(privs.get(index)));
        }
        index++;
      }
    } else {
      for (String str : constPattern.split(s.toString(), -1)) {
        if ("1".equals(str)) {
          result.add(new Text(privs.get(index)));
        }
        index++;
      }
    }
    return result;
  }

  @Override public String getDisplayString(String[] children) {
    assert (children.length == 2);
    return getStandardDisplayString("split_map_privs", children);
  }

}
