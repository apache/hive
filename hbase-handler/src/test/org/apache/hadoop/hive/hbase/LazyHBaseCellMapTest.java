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

package org.apache.hadoop.hive.hbase;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hive.serde2.lazy.LazyFactory;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.LazyMapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Text;

import junit.framework.TestCase;

public class LazyHBaseCellMapTest extends TestCase {
	public static final byte[] TEST_ROW = Bytes.toBytes("test-row");
	public static final byte[] COLUMN_FAMILY = Bytes.toBytes("a");
	public static final String QUAL_PREFIX = "col_";


	public void testInitColumnPrefix() throws Exception {
		Text nullSequence = new Text("\\N");
		ObjectInspector oi = LazyFactory.createLazyObjectInspector(
				TypeInfoUtils.getTypeInfosFromTypeString("map<string,string>").get(0),
				new byte[] { (byte) 1, (byte) 2 }, 0, nullSequence, false, (byte) 0);

		LazyHBaseCellMap b = new LazyHBaseCellMap((LazyMapObjectInspector) oi);

		// Initialize a result
		Cell[] cells = new KeyValue[2];

		final String col1="1";
		final String col2="2";
		cells[0] = new KeyValue(TEST_ROW, COLUMN_FAMILY,
				Bytes.toBytes(QUAL_PREFIX+col1), Bytes.toBytes("cfacol1"));
		cells[1]=new KeyValue(TEST_ROW, COLUMN_FAMILY,
				Bytes.toBytes(QUAL_PREFIX+col2), Bytes.toBytes("cfacol2"));

		Result r = Result.create(cells);

		List<Boolean> mapBinaryStorage = new ArrayList<Boolean>();
		mapBinaryStorage.add(false);
		mapBinaryStorage.add(false);

		b.init(r, COLUMN_FAMILY, mapBinaryStorage, Bytes.toBytes(QUAL_PREFIX), true);

		assertNotNull(b.getMapValueElement(new Text(col1)));
		assertNotNull(b.getMapValueElement(new Text(col2)));

	}
}
