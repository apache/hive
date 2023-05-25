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
package org.apache.hadoop.hive.ql.udf.bitmap;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFParamUtils;
import org.apache.hadoop.hive.ql.util.bitmap.RoaringBitmapSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.writableStringObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping.BINARY_GROUP;

@Description(name = "bitmap_from_string", value = "_FUNC_(x) - Convert a input BITMAP to a string by comma separated.",
  extended = "Example:\n"
    + "  > SELECT _FUNC_(bitmap_from_string('1,2,3')) FROM src LIMIT 1;\n"
    + "  '1,2,3'\n"
)
public class UDFBitmapToString extends GenericUDF {

    public final static String FUNC_NAME = "bitmap_from_string";
    private static final Logger LOG = LoggerFactory.getLogger(UDFBitmapToString.class.getName());
    private transient ObjectInspectorConverters.Converter[] converters = new ObjectInspectorConverters.Converter[1];
    private transient PrimitiveObjectInspector.PrimitiveCategory[] inputTypes = new PrimitiveObjectInspector.PrimitiveCategory[1];


    @Override public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        checkArgsSize(arguments, 1, 1);

        checkArgGroups(arguments, 0, inputTypes, BINARY_GROUP);
        GenericUDFParamUtils.obtainBinaryConverter(arguments, 0, inputTypes, converters);

        //    obta(arguments, 0, inputTypes, converters);
        return writableStringObjectInspector;
    }

    @Override public Object evaluate(DeferredObject[] arguments) throws HiveException {
        StringBuilder sb = new StringBuilder();
        try {
            for (int i : RoaringBitmapSerDe.deserialize(GenericUDFParamUtils.getBinaryValue(arguments, 0, converters).getBytes())
                    .toArray()) {
                sb.append(i+",");
            }
        } catch (IOException e) {
            throw new HiveException(e);
        }
        return new Text(sb.length()>0?sb.substring(0,sb.length()-1):sb.toString());
    }

    @Override public String getDisplayString(String[] children) {
        return getStandardDisplayString(FUNC_NAME, children);
    }
    @Override public String getFuncName() {
        return FUNC_NAME;
    }
}