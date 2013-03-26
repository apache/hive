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

package org.apache.hcatalog.hbase;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hive.hbase.HBaseSerDe;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hcatalog.common.HCatConstants;
import org.apache.hcatalog.common.HCatException;
import org.apache.hcatalog.data.DefaultHCatRecord;
import org.apache.hcatalog.data.HCatRecord;
import org.apache.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hcatalog.data.schema.HCatSchema;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test HBaseSerdeResultConverter by manually creating records to convert to and from HBase objects
 */
public class TestHBaseSerDeResultConverter {

    private Properties createProperties() {
        Properties tbl = new Properties();
        // Set the configuration parameters
        tbl.setProperty(Constants.SERIALIZATION_FORMAT, "9");
        tbl.setProperty("columns","key,aint,astring,amap");
        tbl.setProperty("columns.types","string:int:string:map<string,int>");
        tbl.setProperty(HCatConstants.HCAT_DEFAULT_TOPIC_PREFIX+"."+ HBaseSerDe.HBASE_COLUMNS_MAPPING,
                ":key,my_family:my_qualifier1,my_family:my_qualifier2,my_family2:");
        tbl.setProperty(Constants.SERIALIZATION_NULL_FORMAT, "NULL");
        return tbl;
    }

    private HCatSchema createHCatSchema() throws HCatException {
        HCatSchema subSchema = new HCatSchema(new ArrayList<HCatFieldSchema>());
        subSchema.append(new HCatFieldSchema(null, HCatFieldSchema.Type.INT,""));

        HCatSchema schema = new HCatSchema(new ArrayList<HCatFieldSchema>());
        schema.append(new HCatFieldSchema("key", HCatFieldSchema.Type.STRING,""));
        schema.append(new HCatFieldSchema("aint", HCatFieldSchema.Type.INT,""));
        schema.append(new HCatFieldSchema("astring", HCatFieldSchema.Type.STRING,""));
        schema.append(new HCatFieldSchema("amap", HCatFieldSchema.Type.MAP,HCatFieldSchema.Type.STRING,subSchema,""));
        return schema;
    }

    @Test
    public void testDeserialize() throws IOException {
        HBaseSerDeResultConverter converter = new HBaseSerDeResultConverter(createHCatSchema(),
                                                                                                                    null,
                                                                                                                    createProperties(),
                                                                                                                    1l);
        //test integer
        Result result = new Result(new KeyValue[]{new KeyValue(Bytes.toBytes("row"),
                Bytes.toBytes("my_family"),
                Bytes.toBytes("my_qualifier1"),
                0,
                //This is how Hive's SerDe serializes numbers
                Bytes.toBytes("123")),
                //test string
                new KeyValue(Bytes.toBytes("row"),
                        Bytes.toBytes("my_family"),
                        Bytes.toBytes("my_qualifier2"),
                        0,
                        Bytes.toBytes("onetwothree")),
                //test family map
                new KeyValue(Bytes.toBytes("row"),
                        Bytes.toBytes("my_family2"),
                        Bytes.toBytes("one"),
                        0,
                        Bytes.toBytes("1")),
                new KeyValue(Bytes.toBytes("row"),
                        Bytes.toBytes("my_family2"),
                        Bytes.toBytes("two"),
                        0,
                        Bytes.toBytes("2"))});

        HCatRecord record = converter.convert(result);

        assertEquals(Bytes.toString(result.getRow()), record.get(0).toString());
        assertEquals(Integer.valueOf(
                Bytes.toString(
                        result.getValue(Bytes.toBytes("my_family"), Bytes.toBytes("my_qualifier1")))),
                record.get(1));
        assertEquals(Bytes.toString(
                result.getValue(Bytes.toBytes("my_family"), Bytes.toBytes("my_qualifier2"))),
                record.get(2).toString());
        Map<String,Integer> recordMap = (Map<String,Integer>)record.get(3);
        Map<byte[],byte[]> familyMap = result.getFamilyMap(Bytes.toBytes("my_family2"));
        assertEquals(Integer.valueOf(
                Bytes.toString(
                        familyMap.get(Bytes.toBytes("one")))),
                recordMap.get("one"));
        assertEquals(Integer.valueOf(
                Bytes.toString(
                        familyMap.get(Bytes.toBytes("two")))),
                recordMap.get("two"));
    }

    @Test
    public void testSerialize() throws IOException {
        HCatSchema schema = createHCatSchema();
        HBaseSerDeResultConverter converter = new HBaseSerDeResultConverter(schema,
                                                                                                                    null,
                                                                                                                    createProperties(),
                                                                                                                    1l);
        HCatRecord in = new DefaultHCatRecord(4);
        //row key
        in.set(0,"row");
        //test integer
        in.set(1,123);
        //test string
        in.set(2,"onetwothree");
        //test map
        Map<String,Integer>  map = new HashMap<String,Integer>();
        map.put("one",1);
        map.put("two",2);
        in.set(3,map);

        Put put = converter.convert(in);

        assertEquals(in.get(0).toString(),Bytes.toString(put.getRow()));
        assertEquals(in.get(1),
                Integer.valueOf(
                        Bytes.toString(
                                put.get(Bytes.toBytes("my_family"),
                                        Bytes.toBytes("my_qualifier1")).get(0).getValue())));
        assertEquals(1l,
                put.get(Bytes.toBytes("my_family"),
                        Bytes.toBytes("my_qualifier1")).get(0).getTimestamp());
        assertEquals(in.get(2),
                Bytes.toString(
                        put.get(Bytes.toBytes("my_family"),
                                Bytes.toBytes("my_qualifier2")).get(0).getValue()));
        assertEquals(1l,
                put.get(Bytes.toBytes("my_family"),
                        Bytes.toBytes("my_qualifier2")).get(0).getTimestamp());
        assertEquals(map.get("one"),
                Integer.valueOf(
                        Bytes.toString(
                                put.get(Bytes.toBytes("my_family2"),
                                        Bytes.toBytes("one")).get(0).getValue())));
        assertEquals(1l,
                put.get(Bytes.toBytes("my_family2"),
                        Bytes.toBytes("one")).get(0).getTimestamp());
        assertEquals(map.get("two"),
                Integer.valueOf(Bytes.toString(
                        put.get("my_family2".getBytes(),
                                "two".getBytes()).get(0).getValue())));
        assertEquals(1l,
                put.get(Bytes.toBytes("my_family2"),
                        Bytes.toBytes("two")).get(0).getTimestamp());
    }

    @Test
    public void testScanColumns() throws IOException{
        HCatSchema schema = createHCatSchema();
        HBaseSerDeResultConverter converter = new HBaseSerDeResultConverter(schema,
                null,
                createProperties());

        String result = converter.getHBaseScanColumns();
        String scanColumns = "my_family:my_qualifier1 my_family:my_qualifier2 my_family2: ";

        assertTrue(scanColumns.equals(result));


    }
}
