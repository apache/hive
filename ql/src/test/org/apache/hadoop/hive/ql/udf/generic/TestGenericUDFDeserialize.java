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

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.messaging.MessageEncoder;
import org.apache.hadoop.hive.metastore.messaging.MessageFactory;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.io.Text;
import org.junit.Test;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

/**
 * TestGenericUDFDeserialize:
 * Utility to test the behaviour of GenericUDFDeserialize.
 */
public class TestGenericUDFDeserialize {

    @Test
    public void testOneArg() throws HiveException {
        GenericUDFDeserialize udf = new GenericUDFDeserialize();
        ObjectInspector valueOI1 = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
        ObjectInspector valueOI2 = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
        UDFArgumentException ex = null;
        try {
            udf.initialize(new ObjectInspector[]{valueOI1});
        } catch (UDFArgumentException e) {
            ex = e;
        }
        assertTrue(ex.getMessage().contains("The function deserialize accepts 2 arguments."));
        assertNotNull("The function deserialize() accepts 2 argument.", ex);
        ex = null;
        try {
            udf.initialize(new ObjectInspector[]{valueOI2, valueOI1});
        } catch (UDFArgumentException e) {
            ex = e;
        }
        assertNull("The function deserialize() accepts 2 argument.", ex);
    }

    @Test
    public void testGZIPBase64Compression() throws HiveException {
        GenericUDFDeserialize udf = new GenericUDFDeserialize();
        udf.initialize(new ObjectInspector[]{PrimitiveObjectInspectorFactory.writableStringObjectInspector,
                PrimitiveObjectInspectorFactory.writableStringObjectInspector});
        GenericUDF.DeferredObject[] args = new GenericUDF.DeferredObject[2];
        String expectedOutput = "test";
        MessageEncoder encoder = MessageFactory.getDefaultInstanceForReplMetrics(new HiveConf());
        String serializedMsg = encoder.getSerializer().serialize(expectedOutput);
        args[0] = new GenericUDF.DeferredJavaObject(new Text(serializedMsg));
        args[1] = new GenericUDF.DeferredJavaObject(new Text(encoder.getMessageFormat()));
        Object actualOutput = udf.evaluate(args).toString();
        assertEquals("deserialize() test", expectedOutput, actualOutput != null ? actualOutput : null);
    }

    @Test
    public void testInvalidCompressionFormat() throws HiveException {
        GenericUDFDeserialize udf = new GenericUDFDeserialize();
        udf.initialize(new ObjectInspector[]{PrimitiveObjectInspectorFactory.writableStringObjectInspector,
                PrimitiveObjectInspectorFactory.writableStringObjectInspector});
        GenericUDF.DeferredObject[] args = new GenericUDF.DeferredObject[2];
        String expectedOutput = "test";
        MessageEncoder encoder = MessageFactory.getDefaultInstanceForReplMetrics(new HiveConf());
        String serializedMsg = encoder.getSerializer().serialize(expectedOutput);
        String compressionFormat = "randomSerialization";
        args[0] = new GenericUDF.DeferredJavaObject(new Text(serializedMsg));
        args[1] = new GenericUDF.DeferredJavaObject(new Text(compressionFormat));
        HiveException ex = null;
        try {
            udf.evaluate(args).toString();
        } catch (HiveException e) {
            ex = e;
        }
        assertNotNull("Invalid message format provided.", ex);
        assertTrue(ex.getMessage().contains("compressionFormat: " + compressionFormat + " is not supported."));
    }
}
