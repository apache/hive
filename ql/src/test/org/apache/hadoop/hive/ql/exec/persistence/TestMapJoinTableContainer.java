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
package org.apache.hadoop.hive.ql.exec.persistence;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Properties;

import org.junit.Assert;

import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinarySerDe;
import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;

public class TestMapJoinTableContainer {
  
  private static final Object[] KEY = new Object[] {new Text("key")};
  private static final Object[] VALUE = new Object[] {new Text("value")};
  private ByteArrayOutputStream baos;
  private ObjectOutputStream out;
  private ObjectInputStream in;
  private MapJoinPersistableTableContainer container;
  private MapJoinTableContainerSerDe containerSerde;
  private MapJoinKeyObject key;
  private MapJoinRowContainer rowContainer;
  @Before
  public void setup() throws Exception {
    key = new MapJoinKeyObject(KEY);
    rowContainer = new MapJoinEagerRowContainer();
    rowContainer.addRow(VALUE);
    baos = new ByteArrayOutputStream();
    out = new ObjectOutputStream(baos);
    
    LazyBinarySerDe keySerde = new LazyBinarySerDe();
    Properties keyProps = new Properties();
    keyProps.put(serdeConstants.LIST_COLUMNS, "v1");
    keyProps.put(serdeConstants.LIST_COLUMN_TYPES, "string");
    SerDeUtils.initializeSerDe(keySerde, null, keyProps, null);
    LazyBinarySerDe valueSerde = new LazyBinarySerDe();
    Properties valueProps = new Properties();
    valueProps.put(serdeConstants.LIST_COLUMNS, "v1");
    valueProps.put(serdeConstants.LIST_COLUMN_TYPES, "string");
    SerDeUtils.initializeSerDe(valueSerde, null, keyProps, null);
    containerSerde = new MapJoinTableContainerSerDe(
        new MapJoinObjectSerDeContext(keySerde, false),
        new MapJoinObjectSerDeContext(valueSerde, false));
    container = new HashMapWrapper();
  }

  @Test
  public void testSerialization() throws Exception {
    container.put(key, rowContainer);
    containerSerde.persist(out, container);
    out.close();
    in = new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray()));
    container = containerSerde.load(in);
    Utilities.testEquality(rowContainer, container.get(key));
  }
  @Test
  public void testDummyContainer() throws Exception {
    MapJoinTableContainerSerDe.persistDummyTable(out);
    out.close();
    in = new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray()));
    container = containerSerde.load(in);
    Assert.assertEquals(0, container.size());
    Assert.assertTrue(container.entrySet().isEmpty());
  }  
}