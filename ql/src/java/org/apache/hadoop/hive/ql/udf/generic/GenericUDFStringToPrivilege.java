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
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveResourceACLs;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * GenericUDFStringToPrivs.
 *
 */

@Description(name = "split_map_privs", value = "_FUNC_(str) - Splits binary str and maps to privilege type "
    + "regex", extended = "Example:\n" + "  > SELECT _FUNC_('0 1 1 0 1 1 0 0 0') FROM src LIMIT 1;\n"
    + "  [\"UPDATE\", \"CREATE\", \"ALTER\", \"INDEX\"]")

/**
 * GenericUDFStringToPrivs.
 * "_FUNC_(str, regex) - Splits binary str and maps to privilege type "
 *      "Example: > SELECT _FUNC_('0 1 1 0 1 1 0 0 0') FROM src LIMIT 1;"
 *     output: "  ["UPDATE", "CREATE", "ALTER", "INDEX"]"
 */
public class GenericUDFStringToPrivilege extends GenericUDF {
  private transient ObjectInspectorConverters.Converter[] converters = new ObjectInspectorConverters.Converter[1];

  //private PrivilegeMap privsMap = new PrivilegeMap();
  private List<HiveResourceACLs.Privilege> privilegesList = Arrays.asList(HiveResourceACLs.Privilege.values());

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    checkArgsSize(arguments, 1, 1);
    checkArgPrimitive(arguments, 0);

    converters[0] = ObjectInspectorConverters
        .getConverter(arguments[0], PrimitiveObjectInspectorFactory.writableStringObjectInspector);

    return ObjectInspectorFactory
        .getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableStringObjectInspector);
  }

  @Override public Object evaluate(DeferredObject[] arguments) throws HiveException {
    assert (arguments.length == 1);

    if (arguments[0].get() == null) {
      return null;
    }

    Text s = (Text) converters[0].convert(arguments[0].get());
    ArrayList<Text> result = new ArrayList<Text>();
    int index = 0;
    //Map<Integer, String> privs = privsMap.getPrivilegeMap();

    for (String str : s.toString().split(" ", -1)) {
      if ("1".equals(str)) {
        result.add(new Text(String.valueOf(privilegesList.get(index))));
        //result.add(new Text(privs.get(index)));
      }
      index++;
    }

    return result;
  }

  @Override protected String getFuncName() {
    return "split_map_privs";
  }

  @Override public String getDisplayString(String[] children) {
    assert (children.length == 1);
    return getStandardDisplayString("split_map_privs", children);
  }

}
