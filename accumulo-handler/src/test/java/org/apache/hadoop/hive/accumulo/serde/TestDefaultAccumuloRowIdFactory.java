/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.accumulo.serde;

import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.accumulo.columns.ColumnEncoding;
import org.apache.hadoop.hive.accumulo.columns.ColumnMapper;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.lazy.LazyObjectBase;
import org.apache.hadoop.hive.serde2.lazy.LazySerDeParameters;
import org.apache.hadoop.hive.serde2.lazy.LazyString;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.LazyMapObjectInspector;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.LazySimpleStructObjectInspector;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyIntObjectInspector;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyPrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyStringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class TestDefaultAccumuloRowIdFactory {

  @Test
  public void testCorrectPrimitiveInspectors() throws SerDeException {
    AccumuloSerDe accumuloSerDe = new AccumuloSerDe();

    Properties properties = new Properties();
    Configuration conf = new Configuration();
    properties.setProperty(AccumuloSerDeParameters.COLUMN_MAPPINGS, ":rowID,cf:cq");
    properties.setProperty(serdeConstants.LIST_COLUMNS, "row,col");
    properties.setProperty(serdeConstants.LIST_COLUMN_TYPES,
        "string,int");

    accumuloSerDe.initialize(conf, properties);

    AccumuloRowIdFactory factory = accumuloSerDe.getParams().getRowIdFactory();
    List<TypeInfo> columnTypes = accumuloSerDe.getParams().getHiveColumnTypes();
    ColumnMapper mapper = accumuloSerDe.getParams().getColumnMapper();
    LazySerDeParameters serDeParams = accumuloSerDe.getParams().getSerDeParameters();

    List<ObjectInspector> OIs = accumuloSerDe.getColumnObjectInspectors(columnTypes, serDeParams, mapper.getColumnMappings(), factory);

    Assert.assertEquals(2, OIs.size());
    Assert.assertEquals(LazyStringObjectInspector.class, OIs.get(0).getClass());
    Assert.assertEquals(LazyIntObjectInspector.class, OIs.get(1).getClass());
  }

  @Test
  public void testCorrectComplexInspectors() throws SerDeException {
    AccumuloSerDe accumuloSerDe = new AccumuloSerDe();

    Properties properties = new Properties();
    Configuration conf = new Configuration();
    properties.setProperty(AccumuloSerDeParameters.COLUMN_MAPPINGS, ":rowID,cf:cq");
    properties.setProperty(serdeConstants.LIST_COLUMNS, "row,col");
    properties.setProperty(serdeConstants.LIST_COLUMN_TYPES,
        "struct<col1:int,col2:int>,map<string,string>");

    accumuloSerDe.initialize(conf, properties);

    AccumuloRowIdFactory factory = accumuloSerDe.getParams().getRowIdFactory();
    List<TypeInfo> columnTypes = accumuloSerDe.getParams().getHiveColumnTypes();
    ColumnMapper mapper = accumuloSerDe.getParams().getColumnMapper();
    LazySerDeParameters serDeParams = accumuloSerDe.getParams().getSerDeParameters();

    List<ObjectInspector> OIs = accumuloSerDe.getColumnObjectInspectors(columnTypes, serDeParams, mapper.getColumnMappings(), factory);

    // Expect the correct OIs
    Assert.assertEquals(2, OIs.size());
    Assert.assertEquals(LazySimpleStructObjectInspector.class, OIs.get(0).getClass());
    Assert.assertEquals(LazyMapObjectInspector.class, OIs.get(1).getClass());

    LazySimpleStructObjectInspector structOI = (LazySimpleStructObjectInspector) OIs.get(0);
    Assert.assertEquals(2, (int) structOI.getSeparator());

    LazyMapObjectInspector mapOI = (LazyMapObjectInspector) OIs.get(1);
    Assert.assertEquals(2, (int) mapOI.getItemSeparator());
    Assert.assertEquals(3, (int) mapOI.getKeyValueSeparator());
  }

  @Test
  public void testBinaryStringRowId() throws SerDeException {
    AccumuloSerDe accumuloSerDe = new AccumuloSerDe();

    Properties properties = new Properties();
    Configuration conf = new Configuration();
    properties.setProperty(AccumuloSerDeParameters.COLUMN_MAPPINGS, ":rowID,cf:cq");
    properties.setProperty(serdeConstants.LIST_COLUMNS, "row,col");
    properties.setProperty(serdeConstants.LIST_COLUMN_TYPES,
        "string,string");
    properties.setProperty(AccumuloSerDeParameters.DEFAULT_STORAGE_TYPE, ColumnEncoding.BINARY.getName());

    accumuloSerDe.initialize(conf, properties);

    DefaultAccumuloRowIdFactory rowIdFactory = new DefaultAccumuloRowIdFactory();
    rowIdFactory.init(accumuloSerDe.getParams(), properties);

    LazyStringObjectInspector oi = LazyPrimitiveObjectInspectorFactory.getLazyStringObjectInspector(false, (byte) '\\');
    LazyObjectBase lazyObj = rowIdFactory.createRowId(oi);
    Assert.assertNotNull(lazyObj);
    Assert.assertTrue(LazyString.class.isAssignableFrom(lazyObj.getClass()));
  }

}
