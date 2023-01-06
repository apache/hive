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

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveWritableObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

import static java.util.Arrays.asList;

public class TestAbstractGenericUDFArrayBase {
    GenericUDF udf = null;

    @Test
    public void testIntArray() throws HiveException {

        initUdf(PrimitiveObjectInspectorFactory.writableIntObjectInspector);
        Object i1 = new IntWritable(3);
        Object i2 = new IntWritable(1);
        Object i3 = new IntWritable(2);
        Object i4 = new IntWritable(4);
        Object i5 = new IntWritable(-4);
        //Object i5 = new IntWritable(null);
        if (udf instanceof GenericUDFArrayMax) {
            runAndVerify(asList(i1, i2, i3, i4, i5), i4);
        }
        if (udf instanceof GenericUDFArrayMin) {
            runAndVerify(asList(i1, i2, i3, i4, i5), i5);
        }
    }

    @Test
    public void testFloatArray() throws HiveException {
        initUdf(PrimitiveObjectInspectorFactory.writableFloatObjectInspector);
        Object i1 = new FloatWritable(5.3f);
        Object i2 = new FloatWritable(1.1f);
        Object i3 = new FloatWritable(3.3f);
        Object i4 = new FloatWritable(2.20f);
        if (udf instanceof GenericUDFArrayMax) {
            runAndVerify(asList(i1, i2, i3, i4), i1);
        }
        if (udf instanceof GenericUDFArrayMin) {
            runAndVerify(asList(i1, i2, i3, i4), i2);
        }
    }

    @Test
    public void testDoubleArray() throws HiveException {
        initUdf(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);
        Object i1 = new DoubleWritable(3.31234567890);
        Object i2 = new DoubleWritable(1.11234567890);
        Object i3 = new DoubleWritable(4.31234567890);
        Object i4 = new DoubleWritable(2.21234567890);

        if (udf instanceof GenericUDFArrayMax) {
            runAndVerify(asList(i1, i2, i3, i4), i3);
        }
        if (udf instanceof GenericUDFArrayMin) {
            runAndVerify(asList(i1, i2, i3, i4), i2);
        }
    }

    @Test
    public void testLongArray() throws HiveException {
        initUdf(PrimitiveObjectInspectorFactory.writableLongObjectInspector);
        Object i1 = new LongWritable(331234567890L);
        Object i2 = new LongWritable(911234567890L);
        Object i3 = new LongWritable(131234567890L);
        Object i4 = new LongWritable(221234567890L);

        if (udf instanceof GenericUDFArrayMax) {
            runAndVerify(asList(i1, i2, i3, i4), i2);
        }
        if (udf instanceof GenericUDFArrayMin) {
            runAndVerify(asList(i1, i2, i3, i4), i3);
        }
    }

    private void initUdf(AbstractPrimitiveWritableObjectInspector writableIntObjectInspector) throws UDFArgumentException {
        ObjectInspector[] inputOIs = {
                ObjectInspectorFactory.getStandardListObjectInspector(writableIntObjectInspector)
        };
        if (udf != null ) {
            udf.initialize(inputOIs);
        }
    }

    private void runAndVerify(List<Object> actual, Object expected)
            throws HiveException {
        GenericUDF.DeferredJavaObject[] args = {new GenericUDF.DeferredJavaObject(actual)};
        Object result = udf.evaluate(args);

        Assert.assertEquals("Max/Min value", expected, result);
    }

}
