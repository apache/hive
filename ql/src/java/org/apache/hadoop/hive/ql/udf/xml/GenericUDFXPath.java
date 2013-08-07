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

package org.apache.hadoop.hive.ql.udf.xml;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;
import org.w3c.dom.NodeList;

@Description(
    name = "xpath",
    value = "_FUNC_(xml, xpath) - Returns a string array of values within xml nodes that match the xpath expression",
    extended = "Example:\n"
    + "  > SELECT _FUNC_('<a><b>b1</b><b>b2</b><b>b3</b><c>c1</c><c>c2</c></a>', 'a/text()') FROM src LIMIT 1\n"
    + "  []\n"
    + "  > SELECT _FUNC_('<a><b>b1</b><b>b2</b><b>b3</b><c>c1</c><c>c2</c></a>', 'a/b/text()') FROM src LIMIT 1\n"
    + "  [\"b1\",\"b2\",\"b3\"]\n"
    + "  > SELECT _FUNC_('<a><b>b1</b><b>b2</b><b>b3</b><c>c1</c><c>c2</c></a>', 'a/c/text()') FROM src LIMIT 1\n"
    + "  [\"c1\",\"c2\"]"
    )
public class GenericUDFXPath extends GenericUDF {

  private static final List<Text> emptyResult = Collections.<Text>emptyList();

  private transient final UDFXPathUtil xpath = new UDFXPathUtil();
  private final List<Text> result = new ArrayList<Text>(10);

  private transient Converter converterArg0;
  private transient Converter converterArg1;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments)
      throws UDFArgumentException {
    if (arguments.length != 2) {
      throw new UDFArgumentException("Invalid number of arguments.");
    }

    converterArg0 = ObjectInspectorConverters.getConverter(arguments[0],
        PrimitiveObjectInspectorFactory.writableStringObjectInspector);
    converterArg1 = ObjectInspectorConverters.getConverter(arguments[1],
        PrimitiveObjectInspectorFactory.writableStringObjectInspector);

    return ObjectInspectorFactory
        .getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableStringObjectInspector);
  }

  private List<Text> eval(String xml, String path) {
    NodeList nodeList = xpath.evalNodeList(xml, path);
    if (nodeList == null) {
      return emptyResult;
    }

    result.clear();
    for (int i = 0; i < nodeList.getLength(); i++) {
      String value = nodeList.item(i).getNodeValue();
      if (value != null) {
        result.add(new Text(value));
      }
    }

    return result;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    assert (arguments.length == 2);
    String xml = converterArg0.convert(arguments[0].get()).toString();
    String path = converterArg1.convert(arguments[1].get()).toString();
    return eval(xml, path);
  }

  @Override
  public String getDisplayString(String[] children) {
    StringBuilder builder = new StringBuilder();
    builder.append("array (");
    if (children.length > 0) {
      builder.append("'");
      builder.append(StringUtils.join(children, "','"));
      builder.append("'");
    }
    builder.append(")");
    return builder.toString();
  }
}
