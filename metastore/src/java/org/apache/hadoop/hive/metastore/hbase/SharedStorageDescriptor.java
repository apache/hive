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
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.SkewedInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * A {@link org.apache.hadoop.hive.metastore.api.StorageDescriptor} with most of it's content
 * shared.  Location and parameters are left alone, everything else is redirected to a shared
 * reference in the cache.
 */
public class SharedStorageDescriptor extends StorageDescriptor {
  static final private Log LOG = LogFactory.getLog(SharedStorageDescriptor.class.getName());
  StorageDescriptor shared;

  SharedStorageDescriptor() {
  }

  public SharedStorageDescriptor(SharedStorageDescriptor that) {
    this.setLocation(that.getLocation());
    this.setParameters(that.getParameters());
    this.shared = that.shared;
  }

  void readShared(byte[] hash) throws IOException {
    shared = HBaseReadWrite.getInstance().getStorageDescriptor(hash);
  }

  @Override
  public List<FieldSchema> getCols() {
    return shared.getCols();
  }

  @Override
  public int getColsSize() {
    return shared.getColsSize();
  }

  @Override
  public Iterator<FieldSchema> getColsIterator() {
    return shared.getColsIterator();
  }

  @Override
  public String getInputFormat() {
    return shared.getInputFormat();
  }

  @Override
  public String getOutputFormat() {
    return shared.getOutputFormat();
  }

  @Override
  public boolean isCompressed() {
    return shared.isCompressed();
  }

  @Override
  public int getNumBuckets() {
    return shared.getNumBuckets();
  }

  @Override
  public SerDeInfo getSerdeInfo() {
    return shared.getSerdeInfo();
  }

  @Override
  public List<String> getBucketCols() {
    return shared.getBucketCols();
  }

  @Override
  public int getBucketColsSize() {
    return shared.getBucketColsSize();
  }

  @Override
  public Iterator<String> getBucketColsIterator() {
    return shared.getBucketColsIterator();
  }

  @Override
  public List<Order> getSortCols() {
    return shared.getSortCols();
  }

  @Override
  public int getSortColsSize() {
    return shared.getSortColsSize();
  }

  @Override
  public Iterator<Order> getSortColsIterator() {
    return shared.getSortColsIterator();
  }

  @Override
  public SkewedInfo getSkewedInfo() {
    return shared.getSkewedInfo();
  }

  @Override
  public boolean isStoredAsSubDirectories() {
    return shared.isStoredAsSubDirectories();
  }
}
