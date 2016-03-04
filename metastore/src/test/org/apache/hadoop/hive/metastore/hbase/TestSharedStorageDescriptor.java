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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hive.metastore.hbase;

import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.SkewedInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 *
 */
public class TestSharedStorageDescriptor {
  private static final Logger LOG = LoggerFactory.getLogger(TestHBaseStore.class.getName());


  @Test
  public void changeOnSerde() {
    StorageDescriptor sd = new StorageDescriptor();
    SerDeInfo serde = new SerDeInfo();
    serde.setName("serde");
    sd.setSerdeInfo(serde);
    SharedStorageDescriptor ssd = new SharedStorageDescriptor();
    ssd.setShared(sd);
    ssd.getSerdeInfo().setName("different");
    Assert.assertFalse(sd.getSerdeInfo() == ssd.getSerdeInfo());
    Assert.assertEquals("serde", serde.getName());
    Assert.assertEquals("different", ssd.getSerdeInfo().getName());
    Assert.assertEquals("serde", sd.getSerdeInfo().getName());
  }

  @Test
  public void changeOnSkewed() {
    StorageDescriptor sd = new StorageDescriptor();
    SkewedInfo skew = new SkewedInfo();
    sd.setSkewedInfo(skew);
    SharedStorageDescriptor ssd = new SharedStorageDescriptor();
    ssd.setShared(sd);
    ssd.setSkewedInfo(new SkewedInfo());
    Assert.assertFalse(sd.getSkewedInfo() == ssd.getSkewedInfo());
  }

  @Test
  public void changeOnUnset() {
    StorageDescriptor sd = new StorageDescriptor();
    SkewedInfo skew = new SkewedInfo();
    sd.setSkewedInfo(skew);
    SharedStorageDescriptor ssd = new SharedStorageDescriptor();
    ssd.setShared(sd);
    ssd.unsetSkewedInfo();
    Assert.assertFalse(sd.getSkewedInfo() == ssd.getSkewedInfo());
  }

  @Test
  public void changeOrder() {
    StorageDescriptor sd = new StorageDescriptor();
    sd.addToSortCols(new Order("fred", 1));
    SharedStorageDescriptor ssd = new SharedStorageDescriptor();
    ssd.setShared(sd);
    ssd.getSortCols().get(0).setOrder(2);
    Assert.assertFalse(sd.getSortCols() == ssd.getSortCols());
    Assert.assertEquals(2, ssd.getSortCols().get(0).getOrder());
    Assert.assertEquals(1, sd.getSortCols().get(0).getOrder());
  }

  @Test
  public void unsetOrder() {
    StorageDescriptor sd = new StorageDescriptor();
    sd.addToSortCols(new Order("fred", 1));
    SharedStorageDescriptor ssd = new SharedStorageDescriptor();
    ssd.setShared(sd);
    ssd.unsetSortCols();
    Assert.assertFalse(sd.getSortCols() == ssd.getSortCols());
    Assert.assertEquals(0, ssd.getSortColsSize());
    Assert.assertEquals(1, sd.getSortColsSize());
  }

  @Test
  public void changeBucketList() {
    StorageDescriptor sd = new StorageDescriptor();
    sd.addToBucketCols(new String("fred"));
    SharedStorageDescriptor ssd = new SharedStorageDescriptor();
    ssd.setShared(sd);
    List<String> list = ssd.getBucketCols();
    list.add(new String("bob"));
    Assert.assertFalse(sd.getBucketCols() == ssd.getBucketCols());
    Assert.assertEquals(2, ssd.getBucketColsSize());
    Assert.assertEquals(1, sd.getBucketColsSize());
  }

  @Test
  public void addToColList() {
    StorageDescriptor sd = new StorageDescriptor();
    sd.addToCols(new FieldSchema("fred", "string", ""));
    SharedStorageDescriptor ssd = new SharedStorageDescriptor();
    ssd.setShared(sd);
    ssd.addToCols(new FieldSchema("joe", "int", ""));
    Assert.assertFalse(sd.getCols() == ssd.getCols());
    Assert.assertEquals(2, ssd.getColsSize());
    Assert.assertEquals(1, sd.getColsSize());
  }

  @Test
  public void colIterator() {
    StorageDescriptor sd = new StorageDescriptor();
    sd.addToCols(new FieldSchema("fred", "string", ""));
    SharedStorageDescriptor ssd = new SharedStorageDescriptor();
    ssd.setShared(sd);
    Iterator<FieldSchema> iter = ssd.getColsIterator();
    Assert.assertTrue(iter.hasNext());
    Assert.assertEquals("fred", iter.next().getName());
    Assert.assertFalse(sd.getCols() == ssd.getCols());
  }

  @Test
  public void setReadOnly() {
    StorageDescriptor sd = new StorageDescriptor();
    sd.addToCols(new FieldSchema("fred", "string", ""));
    SharedStorageDescriptor ssd = new SharedStorageDescriptor();
    ssd.setShared(sd);
    ssd.setReadOnly();
    List<FieldSchema> cols = ssd.getCols();
    Assert.assertEquals(1, cols.size());
    Assert.assertTrue(sd.getCols() == ssd.getCols());
  }

}
