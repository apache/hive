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

import java.io.UnsupportedEncodingException;



import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import static org.junit.Assert.assertEquals;
import org.junit.Test;

/**
 * TestGenericUDFDecode.
 */
public class TestGenericUDFDecode {
  @Test
  public void testDecode() throws UnsupportedEncodingException, HiveException {
    String[] charsetNames = {"US-ASCII", "ISO-8859-1", "UTF-8", "UTF-16BE", "UTF-16LE", "UTF-16"};
    for (String charsetName : charsetNames){
      verifyDecode("A sample string", charsetName);
    }
  }

  public void verifyDecode(String string, String charsetName) throws UnsupportedEncodingException, HiveException{
    GenericUDFDecode udf = new GenericUDFDecode();
    byte[] bytes = string.getBytes(charsetName);

    ObjectInspector valueOI = PrimitiveObjectInspectorFactory.javaByteArrayObjectInspector;
    ObjectInspector charsetOI = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    ObjectInspector[] initArguments = {valueOI, charsetOI};
    udf.initialize(initArguments);

    DeferredObject valueObj = new DeferredJavaObject(bytes);
    DeferredObject charsetObj = new DeferredJavaObject(charsetName);
    DeferredObject[] arguments = {valueObj, charsetObj};
    String output = (String) udf.evaluate(arguments);

    assertEquals("Decoding failed for CharSet: " + charsetName, string, output);
  }
}

