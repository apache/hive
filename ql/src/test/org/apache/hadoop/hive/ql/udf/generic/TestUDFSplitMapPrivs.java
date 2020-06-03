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

import junit.framework.TestCase;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;
import org.junit.Test;
import java.util.ArrayList;
import java.util.List;
import static java.util.Arrays.asList;
import static org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import static org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;

/**
*
* This test a test for udf GenericUDFStringToPrivilege.
*
*/
public class TestUDFSplitMapPrivs extends TestCase {
  private final GenericUDFStringToPrivilege udf = new GenericUDFStringToPrivilege();
  private final Object p0 = new Text("SELECT");
  private final Object p1 = new Text("UPDATE");
  private final Object p2 = new Text("CREATE");
  private final Object p3 = new Text("DROP");
  private final Object p4 = new Text("ALTER");
  private final Object p5 = new Text("INDEX");
  private final Object p6 = new Text("LOCK");
  private final Object p7 = new Text("READ");
  private final Object p8 = new Text("WRITE");
  private final Object p9 = new Text("All");


  @Test public void testBinaryStringSplitMapToPrivs() throws HiveException {

    ObjectInspector valueOI0 = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    ObjectInspector[] initArgs = {valueOI0};

    udf.initialize(initArgs);

    DeferredObject args;
    DeferredObject[] evalArgs;

    args = new DeferredJavaObject(new Text("1 0 0 0 0 0 0 0 0 0"));
    evalArgs = new DeferredObject[] {args};
    runAndVerify(asList(p0), evalArgs);

    args = new DeferredJavaObject(new Text("1 1 0 0 0 0 0 0 0 0"));
    evalArgs = new DeferredObject[] {args};
    runAndVerify(asList(p0, p1), evalArgs);

    args = new DeferredJavaObject(new Text("1 1 1 0 0 0 0 0 0 0"));
    evalArgs = new DeferredObject[] {args};
    runAndVerify(asList(p0, p1, p2), evalArgs);

    args = new DeferredJavaObject(new Text("1 1 1 1 0 0 0 0 0 0"));
    evalArgs = new DeferredObject[] {args};
    runAndVerify(asList(p0, p1, p2, p3), evalArgs);

    args = new DeferredJavaObject(new Text("1 1 1 1 1 0 0 0 0 0"));
    evalArgs = new DeferredObject[] {args};
    runAndVerify(asList(p0, p1, p2, p3, p4), evalArgs);

    args = new DeferredJavaObject(new Text("1 1 1 1 1 1 0 0 0 0"));
    evalArgs = new DeferredObject[] {args};
    runAndVerify(asList(p0, p1, p2, p3, p4, p5), evalArgs);

    args = new DeferredJavaObject(new Text("1 1 1 1 1 1 1 0 0 0"));
    evalArgs = new DeferredObject[] {args};
    runAndVerify(asList(p0, p1, p2, p3, p4, p5, p6), evalArgs);

    args = new DeferredJavaObject(new Text("1 1 1 1 1 1 1 1 0 0"));
    evalArgs = new DeferredObject[] {args};
    runAndVerify(asList(p0, p1, p2, p3, p4, p5, p6, p7), evalArgs);

    args = new DeferredJavaObject(new Text("1 0 1 1 1 1 1 1 1 0"));
    evalArgs = new DeferredObject[] {args};
    runAndVerify(asList(p0, p2, p3, p4, p5, p6, p7, p8), evalArgs);

  }

  @Test public void binaryStringMapingShouldFail() throws HiveException {

    ObjectInspector valueOI0 = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    ObjectInspector[] initArgs = {valueOI0};

    udf.initialize(initArgs);
    DeferredObject args;
    DeferredObject[] evalArgs;

    args = new DeferredJavaObject(new Text("1 0 0 0 0 0 0 0 0 0"));
    evalArgs = new DeferredObject[] {args};
    runAndVerifyNotTrue(asList(p1), evalArgs);

    args = new DeferredJavaObject(new Text("1 1 0 0 0 0 0 0 0 0"));
    evalArgs = new DeferredObject[] {args};
    runAndVerifyNotTrue(asList(p0, p5), evalArgs);

  }

  private void runAndVerify(List<Object> expResult, DeferredObject[] evalArgs) throws HiveException {

    ArrayList output = (ArrayList) udf.evaluate(evalArgs);
    assertEquals(expResult, output);
  }

  private void runAndVerifyNotTrue(List<Object> expResult, DeferredObject[] evalArgs) throws HiveException {

    ArrayList output = (ArrayList) udf.evaluate(evalArgs);
    assertNotSame(expResult, output);
  }

}
