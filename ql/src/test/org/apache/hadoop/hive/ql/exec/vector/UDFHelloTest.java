/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.exec.vector;

import org.apache.hadoop.hive.ql.exec.MapredContext;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * UDF to obfuscate input data appending "Hello "
 */
public class UDFHelloTest extends GenericUDF {
  private static final Logger LOG = LoggerFactory.getLogger(UDFHelloTest.class);

  private Text result = new Text();

  private static String greeting = "";

  private ObjectInspectorConverters.Converter[] converters;

  @Override
  public Object evaluate(DeferredObject[] arg0) throws HiveException {

    if (arg0.length != 1) {
      LOG.error("UDFHelloTest expects exactly 1 argument");
      throw new HiveException("UDFHelloTest expects exactly 1 argument");
    }

    if (arg0[0].get() == null) {
      LOG.warn("Empty input");
      return null;
    }

    Text data = (Text) converters[0].convert(arg0[0].get());

    String dataString = data.toString();

    result.set(greeting + dataString);

    return result;
  }

  @Override
  public String getDisplayString(String[] arg0) {
    return "Hello...";
  }

  @Override
  public void configure(MapredContext context) {
    greeting = "Hello ";
  }

  @Override
  public ObjectInspector initialize(ObjectInspector[] arg0) throws UDFArgumentException {
    converters = new ObjectInspectorConverters.Converter[arg0.length];
    for (int i = 0; i < arg0.length; i++) {
      converters[i] = ObjectInspectorConverters.getConverter(arg0[i],
              PrimitiveObjectInspectorFactory.writableStringObjectInspector);
    }

    // evaluate will return a Text object
    return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
  }
}
