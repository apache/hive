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

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.Test;

import java.io.UnsupportedEncodingException;

import static org.junit.Assert.assertEquals;

public class TestGenericUDFConvertCharset {
  @Test public void testConvertCharset() throws UnsupportedEncodingException, HiveException {
    String[] charsetNames = { "US-ASCII", "ISO-8859-1", "UTF-8", "UTF-16BE", "UTF-16LE", "UTF-16" };
    for (String fromCharsetName : charsetNames) {
      for (String toCharsetName : charsetNames) {
        verifyConvertCharset("A sample string", fromCharsetName, toCharsetName);
      }
    }
  }

  public void verifyConvertCharset(String string, String fromCharsetName, String toCharsetName)
      throws UnsupportedEncodingException, HiveException {
    GenericUDFConvertCharset udf = new GenericUDFConvertCharset();
    byte[] bs = string.getBytes(fromCharsetName);
    String expected = new String(bs, toCharsetName);

    ObjectInspector valueOI = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    ObjectInspector fromCharsetOI = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    ObjectInspector toCharsetOI = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    ObjectInspector[] initArguments = { valueOI, fromCharsetOI, toCharsetOI };
    udf.initialize(initArguments);

    GenericUDF.DeferredObject valueObj = new GenericUDF.DeferredJavaObject(string);
    GenericUDF.DeferredObject fromCharsetObj = new GenericUDF.DeferredJavaObject(fromCharsetName);
    GenericUDF.DeferredObject toCharsetObj = new GenericUDF.DeferredJavaObject(toCharsetName);
    GenericUDF.DeferredObject[] arguments = { valueObj, fromCharsetObj, toCharsetObj };
    String output = (String) udf.evaluate(arguments);

    assertEquals("ConvertCharset failed from CharSet " + fromCharsetName + " to CharSet " + toCharsetName, expected,
        output);
  }
}
