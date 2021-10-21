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

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hive.metastore.messaging.MessageDeserializer;
import org.apache.hadoop.hive.metastore.messaging.json.gzip.GzipJSONMessageEncoder;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.io.Text;

/**
 * GenericUDFDeserializeString.
 *
 */
@Description(name = "gzip_json_deserialize",
        value="_FUNC_(message) - Returns deserialized string of gzip compressed + base64 encoded message.",
        extended="Example:\n"
                + "  > SELECT _FUNC_('H4sIAAAAAAAA/ytJLS4BAAx+f9gEAAAA') FROM src LIMIT 1;\n"
                + "  test")
public class GenericUDFGzipJsonDeserialize extends GenericUDF {

    private static final int STRING_IDX = 0;
    private static final int ARG_COUNT = 1; // Number of arguments to this UDF
    private static final String FUNC_NAME = "gzip_json_deserialize"; // External Name

    private transient PrimitiveObjectInspector stringOI = null;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments)
            throws UDFArgumentException {
        if (arguments.length != ARG_COUNT) {
            throw new UDFArgumentException("The function " + FUNC_NAME + " accepts " + ARG_COUNT + " arguments.");
        }
        if (arguments[0].getCategory() != ObjectInspector.Category.PRIMITIVE ||
                PrimitiveObjectInspectorUtils.PrimitiveGrouping.STRING_GROUP != PrimitiveObjectInspectorUtils.getPrimitiveGrouping(
                        ((PrimitiveObjectInspector)arguments[0]).getPrimitiveCategory())){
            throw new UDFArgumentTypeException(0, "The first argument to " + FUNC_NAME + " must be a string/varchar");
        }
        stringOI = (PrimitiveObjectInspector) arguments[STRING_IDX];
        return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        String value = PrimitiveObjectInspectorUtils.getString(arguments[0].get(), stringOI);
        if (value == null) {
            return null;
        }
        MessageDeserializer deserializer = GzipJSONMessageEncoder.getInstance().getDeserializer();
        return deserializer.deSerializeGenericString(value);
    }

    @Override
    public String getDisplayString(String[] children) {
        assert (children.length == ARG_COUNT);
        return getStandardDisplayString(FUNC_NAME, children, ",");
    }
}
