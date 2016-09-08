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

package org.apache.hadoop.hive.ql.udf.generic;

import static java.util.Arrays.asList;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveVarcharWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;


public class TestGenericUDFSortArrayByField {

  private final GenericUDFSortArrayByField udf = new GenericUDFSortArrayByField();

  @Test
  public void testSortPrimitiveTupleOneField() throws HiveException {

    List<ObjectInspector> tuple = new ArrayList<ObjectInspector>();
    tuple.add(PrimitiveObjectInspectorFactory.writableStringObjectInspector);
    tuple.add(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);

    ObjectInspector[] inputOIs =
        { ObjectInspectorFactory.getStandardListObjectInspector(ObjectInspectorFactory
            .getStandardStructObjectInspector(asList("Company", "Salary"), tuple)),
            PrimitiveObjectInspectorFactory.writableHiveVarcharObjectInspector };

    udf.initialize(inputOIs);

    Object i1 = asList(new Text("Facebook"), new DoubleWritable(80223.25));
    Object i2 = asList(new Text("Facebook"), new DoubleWritable(50223.25));
    Object i3 = asList(new Text("Facebook"), new DoubleWritable(40223.25));
    Object i4 = asList(new Text("Facebook"), new DoubleWritable(60223.25));

    HiveVarchar vc = new HiveVarchar();
    vc.setValue("Salary");
    GenericUDF.DeferredJavaObject[] argas =
        { new GenericUDF.DeferredJavaObject(asList(i1, i2, i3, i4)), new GenericUDF.DeferredJavaObject(
            new HiveVarcharWritable(vc)) };

    runAndVerify(argas, asList(i3, i2, i4, i1));
  }

  @Test
  public void testSortPrimitiveTupleOneFieldOrderASC() throws HiveException {

    List<ObjectInspector> tuple = new ArrayList<ObjectInspector>();
    tuple.add(PrimitiveObjectInspectorFactory.writableStringObjectInspector);
    tuple.add(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);

    ObjectInspector[] inputOIs =
        { ObjectInspectorFactory.getStandardListObjectInspector(ObjectInspectorFactory
            .getStandardStructObjectInspector(asList("Company", "Salary"), tuple)),
            PrimitiveObjectInspectorFactory.writableHiveVarcharObjectInspector,
            PrimitiveObjectInspectorFactory.writableHiveVarcharObjectInspector };

    udf.initialize(inputOIs);

    Object i1 = asList(new Text("Facebook"), new DoubleWritable(80223.25));
    Object i2 = asList(new Text("Facebook"), new DoubleWritable(50223.25));
    Object i3 = asList(new Text("Facebook"), new DoubleWritable(40223.25));
    Object i4 = asList(new Text("Facebook"), new DoubleWritable(60223.25));

    HiveVarchar vc = new HiveVarchar();
    vc.setValue("Salary");
    HiveVarchar order = new HiveVarchar();
    order.setValue("ASC");
    GenericUDF.DeferredJavaObject[] argas =
        { new GenericUDF.DeferredJavaObject(asList(i1, i2, i3, i4)),
          new GenericUDF.DeferredJavaObject(new HiveVarcharWritable(vc)),
          new GenericUDF.DeferredJavaObject(new HiveVarcharWritable(order))
        };

    runAndVerify(argas, asList(i3, i2, i4, i1));
  }

  @Test
  public void testSortPrimitiveTupleTwoField() throws HiveException {

    List<ObjectInspector> tuple = new ArrayList<ObjectInspector>();
    tuple.add(PrimitiveObjectInspectorFactory.writableStringObjectInspector);
    tuple.add(PrimitiveObjectInspectorFactory.writableStringObjectInspector);

    ObjectInspector[] inputOIs =
        { ObjectInspectorFactory.getStandardListObjectInspector(ObjectInspectorFactory
            .getStandardStructObjectInspector(asList("Company", "Department"), tuple)),
            PrimitiveObjectInspectorFactory.writableStringObjectInspector,
            PrimitiveObjectInspectorFactory.writableHiveVarcharObjectInspector };

    udf.initialize(inputOIs);

    Object i1 = asList(new Text("Linkedin"), new Text("HR"));
    Object i2 = asList(new Text("Linkedin"), new Text("IT"));
    Object i3 = asList(new Text("Linkedin"), new Text("Finance"));
    Object i4 = asList(new Text("Facebook"), new Text("IT"));
    Object i5 = asList(new Text("Facebook"), new Text("Finance"));
    Object i6 = asList(new Text("Facebook"), new Text("HR"));
    Object i7 = asList(new Text("Google"), new Text("Logistics"));
    Object i8 = asList(new Text("Google"), new Text("Finance"));
    Object i9 = asList(new Text("Google"), new Text("HR"));

    HiveVarchar vc = new HiveVarchar();
    vc.setValue("Department");

    GenericUDF.DeferredJavaObject[] argas =
        { new GenericUDF.DeferredJavaObject(asList(i1, i2, i3, i4, i5, i6, i7, i8, i9)),
        new GenericUDF.DeferredJavaObject(
            new Text("Company")), new GenericUDF.DeferredJavaObject(new HiveVarcharWritable(vc)) };

    runAndVerify(argas, asList(i5, i6, i4, i8, i9, i7, i3, i1, i2));
  }


  @Test
  public void testSortPrimitiveTupleTwoFieldOrderDESC() throws HiveException {

    List<ObjectInspector> tuple = new ArrayList<ObjectInspector>();
    tuple.add(PrimitiveObjectInspectorFactory.writableStringObjectInspector);
    tuple.add(PrimitiveObjectInspectorFactory.writableStringObjectInspector);

    ObjectInspector[] inputOIs =
        { ObjectInspectorFactory.getStandardListObjectInspector(ObjectInspectorFactory
            .getStandardStructObjectInspector(asList("Company", "Department"), tuple)),
            PrimitiveObjectInspectorFactory.writableStringObjectInspector,
            PrimitiveObjectInspectorFactory.writableHiveVarcharObjectInspector,
            PrimitiveObjectInspectorFactory.writableStringObjectInspector};

    udf.initialize(inputOIs);

    Object i1 = asList(new Text("Linkedin"), new Text("HR"));
    Object i2 = asList(new Text("Linkedin"), new Text("IT"));
    Object i3 = asList(new Text("Linkedin"), new Text("Finance"));
    Object i4 = asList(new Text("Facebook"), new Text("IT"));
    Object i5 = asList(new Text("Facebook"), new Text("Finance"));
    Object i6 = asList(new Text("Facebook"), new Text("HR"));
    Object i7 = asList(new Text("Google"), new Text("Logistics"));
    Object i8 = asList(new Text("Google"), new Text("Finance"));
    Object i9 = asList(new Text("Google"), new Text("HR"));

    HiveVarchar vc = new HiveVarchar();
    vc.setValue("Department");

    GenericUDF.DeferredJavaObject[] argas =
        { new GenericUDF.DeferredJavaObject(asList(i1, i2, i3, i4, i5, i6, i7, i8, i9)),
        new GenericUDF.DeferredJavaObject(new Text("Company")),
        new GenericUDF.DeferredJavaObject(new HiveVarcharWritable(vc)),
        new GenericUDF.DeferredJavaObject(new Text("DESC"))};

    runAndVerify(argas, asList(i2, i1, i3, i7, i9, i8, i4, i6, i5));
  }

  @Test
  public void testSortTupleArrayStructOrderDESC() throws HiveException {
    List<ObjectInspector> tuple = new ArrayList<ObjectInspector>();
    tuple.add(PrimitiveObjectInspectorFactory.writableStringObjectInspector);
    tuple.add(ObjectInspectorFactory
        .getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableIntObjectInspector));

    ObjectInspector field =
        PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(TypeInfoFactory.stringTypeInfo,
            new Text("Scores"));

    ObjectInspector orderField =
        PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(TypeInfoFactory.stringTypeInfo,
            new Text("desc"));

    ObjectInspector[] inputOIs =
        { ObjectInspectorFactory.getStandardListObjectInspector(ObjectInspectorFactory
            .getStandardStructObjectInspector(asList("Student", "Scores"), tuple)),
            field,
            orderField
        };

    udf.initialize(inputOIs);

    Object i1 =
        asList(new Text("Foo"), asList(new IntWritable(4), new IntWritable(3), new IntWritable(2), new IntWritable(1)));

    Object i2 =
        asList(new Text("Boo"), asList(new IntWritable(2), new IntWritable(3), new IntWritable(2), new IntWritable(1)));

    Object i3 =
        asList(new Text("Tom"), asList(new IntWritable(10), new IntWritable(3), new IntWritable(2), new IntWritable(1)));

    GenericUDF.DeferredJavaObject[] argas =
        { new GenericUDF.DeferredJavaObject(asList(i1, i2, i3)),
          new GenericUDF.DeferredJavaObject(field),
          new GenericUDF.DeferredJavaObject(orderField),
        };

    runAndVerify(argas, asList(i3, i1, i2));
  }


  private void runAndVerify(GenericUDF.DeferredJavaObject[] args, List<Object> expected) throws HiveException {
    List<Object> result = (List<Object>) udf.evaluate(args);
    Assert.assertEquals("Check size", expected.size(), result.size());
    Assert.assertArrayEquals("Check content", expected.toArray(), result.toArray());
  }
}
