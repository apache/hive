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

package org.apache.hadoop.hive.ql.udf;

import java.io.UnsupportedEncodingException;

import junit.framework.TestCase;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BytesWritable;

public class TestGenericUDFEncode extends TestCase {
  public void testEncode() throws UnsupportedEncodingException, HiveException{
    String[] charsetNames = {"US-ASCII", "ISO-8859-1", "UTF-8", "UTF-16BE", "UTF-16LE", "UTF-16"};
    for (String charsetName : charsetNames){
      verifyEncode("A sample string", charsetName);
    }
  }

  public void verifyEncode(String string, String charsetName) throws UnsupportedEncodingException, HiveException{
    GenericUDFEncode udf = new GenericUDFEncode();
    byte[] expected = string.getBytes(charsetName);

    ObjectInspector valueOI = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    ObjectInspector charsetOI = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    ObjectInspector[] initArguments = {valueOI, charsetOI};
    udf.initialize(initArguments);

    DeferredObject valueObj = new DeferredJavaObject(string);
    DeferredObject charsetObj = new DeferredJavaObject(charsetName);
    DeferredObject[] arguments = {valueObj, charsetObj};
    BytesWritable outputWritable = (BytesWritable) udf.evaluate(arguments);

    byte[] output = outputWritable.getBytes();
    assertTrue("Encoding failed for CharSet: " + charsetName, expected.length == outputWritable.getLength());
    for (int i = 0; i < expected.length; i++){
      assertEquals("Encoding failed for CharSet: " + charsetName, expected[i], output[i]);
    }
  }
}
