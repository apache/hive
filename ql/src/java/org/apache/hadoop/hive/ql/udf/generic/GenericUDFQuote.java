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
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorConverter.TextConverter;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;

/**
 * Source for GenericUDFQuote.
 */
@Description(name = "quote", value = "_FUNC_() - Returns the Quoted string" + "Example:\n "
    + "  > SELECT _FUNC_('Don't') FROM src LIMIT 1;\n" + "  'Don\'t'\n Including Single quotes\n")
public class GenericUDFQuote extends GenericUDF {
  private transient TextConverter converter;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length != 1) {
      throw new UDFArgumentException("QUOTE() requires one value argument. Found :" + arguments.length);
    }
    PrimitiveObjectInspector argumentOI;
    if (arguments[0] instanceof PrimitiveObjectInspector) {
      argumentOI = (PrimitiveObjectInspector) arguments[0];
    } else {
      throw new UDFArgumentException("QUOTE() takes only primitive types. Found " + arguments[0].getTypeName());
    }
    converter = new TextConverter(argumentOI);
    return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
  }

  @Override
  public Text evaluate(DeferredObject[] arguments) throws HiveException {
    final String qtChar = "'";
    final String qtCharRep = "\\\\'";

    Object valObject = arguments[0].get();
    if (valObject == null) {
      return null;
    }
    String val = ((Text) converter.convert(valObject)).toString();
    String sp = (String) val.replaceAll(qtChar, qtCharRep);
    sp = qtChar + sp + qtChar;
    return new Text(sp);
  }

  @Override
  public String getDisplayString(String[] children) {
    return getStandardDisplayString("quote", children);
  }
}
