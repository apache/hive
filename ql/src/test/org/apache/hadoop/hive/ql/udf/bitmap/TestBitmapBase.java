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
 * distributed under the License is distributed on an "import org.apache.hadoop.hive.ql.exec.FunctionInfo;
AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.udf.bitmap;

import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.IntWritable;

import java.util.Arrays;

import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.writableIntObjectInspector;

public class TestBitmapBase {
    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(TestBitmapBase.class);

    public Object getBitmapFromInts(int... ints) {
        int length = ints == null ? 0 : ints.length;
        UDFBitmap udfBitmap = new UDFBitmap();
        ObjectInspector[] arguments = new ObjectInspector[length]; //
        Arrays.fill(arguments, writableIntObjectInspector);
        GenericUDF.DeferredObject[] value = new GenericUDF.DeferredObject[length];
        for (int i = 0; i < length; i++) {
            value[i] = new GenericUDF.DeferredJavaObject(new IntWritable(ints[i]));
        }
        try {
            udfBitmap.initialize(arguments);
            Object evaluate = udfBitmap.evaluate(value);
            LOG.debug("raw value:{}, bitmap binary result:{}", ints, evaluate);
            return evaluate;
        } catch (IndexOutOfBoundsException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}