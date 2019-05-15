/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.serde2.avro;

import static org.junit.Assert.assertNull;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.serde2.lazy.ByteArrayRef;
import org.apache.hadoop.hive.serde2.lazy.LazyStruct;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.LazyObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyPrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.io.Text;
import org.junit.Test;

public class TestAvroLazyObjectInspector {

	@Test
	public void testEmptyData(){
		List<String> fieldNames = new ArrayList<String>();
		fieldNames.add("myField");

		List<ObjectInspector> ois = new ArrayList<ObjectInspector>();
    ois.add(LazyPrimitiveObjectInspectorFactory.getLazyStringObjectInspector(false, Byte.valueOf((byte) 0)));

		AvroLazyObjectInspector aloi = new AvroLazyObjectInspector(fieldNames, ois, null, (byte)0, new Text(), false, false, (byte)0);
		LazyStruct lazyStruct = new LazyStruct(LazyObjectInspectorFactory.getLazySimpleStructObjectInspector(fieldNames, ois, (byte)0, new Text(), false, false, (byte)0));

		ByteArrayRef byteArrayRef = new ByteArrayRef();
		byteArrayRef.setData(new byte[0]); // set data to empty explicitly
		lazyStruct.init(byteArrayRef, 0, 0);

		assertNull(aloi.getStructFieldData(lazyStruct, new TestStructField()));
	}

	class TestStructField implements StructField {

		@Override
		public String getFieldName() {
			return "testfield";
		}

		@Override
		public ObjectInspector getFieldObjectInspector() {
      return LazyPrimitiveObjectInspectorFactory.getLazyStringObjectInspector(false, Byte.valueOf((byte) 0));
		}

		@Override
		public int getFieldID() {
			return 0;
		}

		@Override
		public String getFieldComment() {
			return null;
    }
	}
}
