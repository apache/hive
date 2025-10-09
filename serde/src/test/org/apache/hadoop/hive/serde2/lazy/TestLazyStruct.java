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
package org.apache.hadoop.hive.serde2.lazy;

import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyObjectInspectorParameters;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyObjectInspectorParametersImpl;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * TestLazyStruct.<br/>
 */
public class TestLazyStruct {

    @Test
    public void testParseMultiDelimit() throws Throwable {
        try {
            // single field named id
            List<String> structFieldNames = new ArrayList<>();
            structFieldNames.add("id");
            // field type is string
            List<TypeInfo> fieldTypes = new ArrayList<>();
            PrimitiveTypeInfo primitiveTypeInfo = new PrimitiveTypeInfo();
            primitiveTypeInfo.setTypeName("string");
            fieldTypes.add(primitiveTypeInfo);
            // separators + escapeChar => "|"
            byte[] separators = new byte[]{124, 2, 3, 4, 5, 6, 7, 8};

            // sequence =>"\N"
            Text sequence = new Text();
            sequence.set(new byte[]{92, 78});

            // create lazy object inspector parameters
            LazyObjectInspectorParameters lazyObjectInspectorParameters = new LazyObjectInspectorParametersImpl(false, (byte) '0',
                    false, null, separators, sequence);
            // create a lazy struct inspector
            ObjectInspector lazyStructInspector = LazyFactory.createLazyStructInspector(structFieldNames, fieldTypes, lazyObjectInspectorParameters);
            LazyStruct lazyStruct = (LazyStruct) LazyFactory.createLazyObject(lazyStructInspector);

            // origin row data
            String rowData = "1|@|";
            // row field delimiter
            String fieldDelimiter = "|@|";

            // parse row use multi delimit
            lazyStruct.parseMultiDelimit(rowData.getBytes(StandardCharsets.UTF_8),
                    fieldDelimiter.getBytes(StandardCharsets.UTF_8));

            // check the first field and second field start position index
            // before fix result: 0,1
            // after fix result: 0,2
            Assert.assertArrayEquals(new int[]{0, 2}, lazyStruct.startPosition);
        } catch (Throwable e) {
            e.printStackTrace();
            throw e;
        }

    }
}