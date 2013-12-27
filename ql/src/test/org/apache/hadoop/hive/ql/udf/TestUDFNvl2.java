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

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;

public class TestUDFNvl2 {
    private static final String AVAILABLE = "Available";
    private static final String NA = "N/A";

    @Test
    public void testNotNull() throws HiveException {
        UDFNvl2 udf = new UDFNvl2();

        ObjectInspector[] inputOIs = {
                PrimitiveObjectInspectorFactory.writableStringObjectInspector,
                PrimitiveObjectInspectorFactory.writableStringObjectInspector,
                PrimitiveObjectInspectorFactory.writableStringObjectInspector,
        };

        GenericUDF.DeferredObject[] args = {
                new GenericUDF.DeferredJavaObject( new Text("not null text") ),
                new GenericUDF.DeferredJavaObject( new Text(AVAILABLE) ),
                new GenericUDF.DeferredJavaObject( new Text(NA) ),
        };

        PrimitiveObjectInspector oi = (PrimitiveObjectInspector) udf.initialize(inputOIs);
        Assert.assertEquals(TypeInfoFactory.stringTypeInfo, oi.getTypeInfo());
        Text res = (Text) udf.evaluate(args);
        Assert.assertEquals(AVAILABLE, res.toString());
    }

    @Test
    public void testNull() throws HiveException {
        UDFNvl2 udf = new UDFNvl2();

        ObjectInspector[] inputOIs = {
                PrimitiveObjectInspectorFactory.writableStringObjectInspector,
                PrimitiveObjectInspectorFactory.writableStringObjectInspector,
                PrimitiveObjectInspectorFactory.writableStringObjectInspector,
        };

        GenericUDF.DeferredObject[] args = {
                new GenericUDF.DeferredJavaObject( null ),
                new GenericUDF.DeferredJavaObject( new Text(AVAILABLE) ),
                new GenericUDF.DeferredJavaObject( new Text(NA) ),
        };

        PrimitiveObjectInspector oi = (PrimitiveObjectInspector) udf.initialize(inputOIs);
        Assert.assertEquals(TypeInfoFactory.stringTypeInfo, oi.getTypeInfo());
        Text res = (Text) udf.evaluate(args);
        Assert.assertEquals(NA, res.toString());
    }

    @Test
    public void testNotNullInt() throws HiveException {
        UDFNvl2 udf = new UDFNvl2();

        ObjectInspector[] inputOIs = {
                PrimitiveObjectInspectorFactory.writableIntObjectInspector,
                PrimitiveObjectInspectorFactory.writableIntObjectInspector,
                PrimitiveObjectInspectorFactory.writableIntObjectInspector,
        };

        GenericUDF.DeferredObject[] args = {
                new GenericUDF.DeferredJavaObject( new IntWritable(12)),
                new GenericUDF.DeferredJavaObject( new IntWritable(1) ),
                new GenericUDF.DeferredJavaObject( new IntWritable(0) ),
        };

        PrimitiveObjectInspector oi = (PrimitiveObjectInspector) udf.initialize(inputOIs);
        Assert.assertEquals(TypeInfoFactory.intTypeInfo, oi.getTypeInfo());
        IntWritable res = (IntWritable) udf.evaluate(args);
        Assert.assertEquals(1, res.get());
    }
}
