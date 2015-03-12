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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.SkewedInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;


/**
 *
 */
public class TestSharedStorageDescriptor {
  private static final Log LOG = LogFactory.getLog(TestHBaseStore.class.getName());


  @Test
  public void location() {
    StorageDescriptor sd = new StorageDescriptor();
    SharedStorageDescriptor ssd = new SharedStorageDescriptor();
    ssd.setLocation("here");
    ssd.setShared(sd);
    ssd.setLocation("there");
    Assert.assertTrue(sd == ssd.getShared());
  }

  @Test
  public void changeOnInputFormat() {
    StorageDescriptor sd = new StorageDescriptor();
    sd.setInputFormat("input");
    SharedStorageDescriptor ssd = new SharedStorageDescriptor();
    ssd.setShared(sd);
    Assert.assertEquals("input", ssd.getInputFormat());
    ssd.setInputFormat("different");
    Assert.assertFalse(sd == ssd.getShared());
    Assert.assertEquals("input", sd.getInputFormat());
    Assert.assertEquals("different", ssd.getInputFormat());
    Assert.assertEquals("input", sd.getInputFormat());
  }

  @Test
  public void changeOnSerde() {
    StorageDescriptor sd = new StorageDescriptor();
    SerDeInfo serde = new SerDeInfo();
    serde.setName("serde");
    sd.setSerdeInfo(serde);
    SharedStorageDescriptor ssd = new SharedStorageDescriptor();
    ssd.setShared(sd);
    Assert.assertEquals("serde", ssd.getSerdeInfo().getName());
    ssd.getSerdeInfo().setName("different");
    Assert.assertFalse(sd == ssd.getShared());
    Assert.assertEquals("serde", serde.getName());
    Assert.assertEquals("different", ssd.getSerdeInfo().getName());
    Assert.assertEquals("serde", sd.getSerdeInfo().getName());
  }

  @Test
  public void multipleChangesDontCauseMultipleCopies() {
    StorageDescriptor sd = new StorageDescriptor();
    sd.setInputFormat("input");
    sd.setOutputFormat("output");
    SharedStorageDescriptor ssd = new SharedStorageDescriptor();
    ssd.setShared(sd);
    Assert.assertEquals("input", ssd.getInputFormat());
    ssd.setInputFormat("different");
    Assert.assertFalse(sd == ssd.getShared());
    Assert.assertEquals("input", sd.getInputFormat());
    Assert.assertEquals("different", ssd.getInputFormat());
    StorageDescriptor keep = ssd.getShared();
    ssd.setOutputFormat("different_output");
    Assert.assertEquals("different", ssd.getInputFormat());
    Assert.assertEquals("different_output", ssd.getOutputFormat());
    Assert.assertEquals("output", sd.getOutputFormat());
    Assert.assertTrue(keep == ssd.getShared());
  }

  @Test
  public void changeOrder() {
    StorageDescriptor sd = new StorageDescriptor();
    sd.addToSortCols(new Order("fred", 1));
    SharedStorageDescriptor ssd = new SharedStorageDescriptor();
    ssd.setShared(sd);
    Assert.assertEquals(1, ssd.getSortCols().get(0).getOrder());
    ssd.getSortCols().get(0).setOrder(2);
    Assert.assertFalse(sd == ssd.getShared());
    Assert.assertEquals(2, ssd.getSortCols().get(0).getOrder());
    Assert.assertEquals(1, sd.getSortCols().get(0).getOrder());
  }

  @Test
  public void changeOrderList() {
    StorageDescriptor sd = new StorageDescriptor();
    sd.addToSortCols(new Order("fred", 1));
    SharedStorageDescriptor ssd = new SharedStorageDescriptor();
    ssd.setShared(sd);
    Assert.assertEquals(1, ssd.getSortCols().get(0).getOrder());
    List<Order> list = ssd.getSortCols();
    list.add(new Order("bob", 2));
    Assert.assertFalse(sd == ssd.getShared());
    Assert.assertEquals(2, ssd.getSortColsSize());
    Assert.assertEquals(1, sd.getSortColsSize());
  }

}
