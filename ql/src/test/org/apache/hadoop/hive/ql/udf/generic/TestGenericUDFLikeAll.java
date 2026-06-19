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

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.io.HiveVarcharWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.junit.Test;


public class TestGenericUDFLikeAll {
  GenericUDFLikeAll udf = null;

  @Test
  public void testTrue() throws HiveException {
    udf = new GenericUDFLikeAll();

    ObjectInspector valueOIOne = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    ObjectInspector valueOITwo = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    ObjectInspector valueOIThree = PrimitiveObjectInspectorFactory.writableHiveVarcharObjectInspector;
    ObjectInspector[] arguments = { valueOIOne, valueOITwo, valueOIThree };

    udf.initialize(arguments);
    DeferredJavaObject valueObjOne = new DeferredJavaObject(new Text("abc"));
    DeferredJavaObject valueObjTwo = new DeferredJavaObject(new Text("%b%"));

    HiveVarchar vc = new HiveVarchar();
    vc.setValue("a%");
    GenericUDF.DeferredJavaObject[] args =
        { valueObjOne, valueObjTwo, new GenericUDF.DeferredJavaObject(new HiveVarcharWritable(vc)) };

    BooleanWritable output = (BooleanWritable) udf.evaluate(args);

    assertEquals(true, output.get());

  }

  @Test(expected = UDFArgumentException.class)
  public void expectException() throws HiveException {
    udf = new GenericUDFLikeAll();
    ObjectInspector valueOIOne = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    ObjectInspector[] arguments = { valueOIOne };
    udf.initialize(arguments);
  }

  @Test
  public void testNull() throws HiveException {
    udf = new GenericUDFLikeAll();
    ObjectInspector valueOIOne = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    ObjectInspector valueOITwo = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    ObjectInspector valueOIThree = PrimitiveObjectInspectorFactory.writableVoidObjectInspector;
    ObjectInspector[] arguments = { valueOIOne, valueOITwo, valueOIThree };
    udf.initialize(arguments);
    DeferredObject valueObjOne = new DeferredJavaObject(new Text("linkedin"));
    DeferredObject valueObjTwo = new DeferredJavaObject(new Text("%oo%"));
    DeferredObject valueObjThree = new DeferredJavaObject(NullWritable.get());
    DeferredObject[] args = { valueObjOne, valueObjTwo, valueObjThree };
    BooleanWritable output = (BooleanWritable) udf.evaluate(args);
    assertEquals(null, output);
  }

}