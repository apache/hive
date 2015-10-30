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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.SkewedInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * A {@link org.apache.hadoop.hive.metastore.api.StorageDescriptor} with most of it's content
 * shallow copied from the underlying storage descriptor.  Location and parameters are left alone.
 * To avoid issues when users change the contents, all lists and nested structures (cols, serde,
 * buckets, sortCols, and skewed) are deep copied when they are accessed for reading or writing.
 * (It has to be done on read as well because there's no way to guarantee the user won't change the
 * nested structure or list, which would result in changing every storage descriptor sharing that
 * structure.)  Users wishing better performance can call setReadyOnly(), which will prevent the
 * copies.
 */
public class SharedStorageDescriptor extends StorageDescriptor {
  static final private Logger LOG = LoggerFactory.getLogger(SharedStorageDescriptor.class.getName());
  private boolean colsCopied = false;
  private boolean serdeCopied = false;
  private boolean bucketsCopied = false;
  private boolean sortCopied = false;
  private boolean skewedCopied = false;

  SharedStorageDescriptor() {
  }

  void setShared(StorageDescriptor shared) {
    if (shared.getCols() != null) super.setCols(shared.getCols());
    // Skip location
    if (shared.getInputFormat() != null) super.setInputFormat(shared.getInputFormat());
    if (shared.getOutputFormat() != null) super.setOutputFormat(shared.getOutputFormat());
    super.setCompressed(shared.isCompressed());
    super.setNumBuckets(shared.getNumBuckets());
    if (shared.getSerdeInfo() != null) super.setSerdeInfo(shared.getSerdeInfo());
    if (shared.getBucketCols() != null) super.setBucketCols(shared.getBucketCols());
    if (shared.getSortCols() != null) super.setSortCols(shared.getSortCols());
    // skip parameters
    if (shared.getSkewedInfo() != null) super.setSkewedInfo(shared.getSkewedInfo());
    super.setStoredAsSubDirectories(shared.isStoredAsSubDirectories());
  }

  /**
   * Promise that you'll only use this shared storage descriptor in a read only mode.
   * This prevents the copies of the nested structures and lists when reading them.  However, the
   * caller must not change the structures or lists returned to it, as this will change all
   * storage descriptor sharing that list.
   */
  public void setReadOnly() {
    colsCopied = serdeCopied = bucketsCopied = sortCopied = skewedCopied = true;
  }

  @Override
  public void addToCols(FieldSchema fs) {
    copyCols();
    super.addToCols(fs);
  }

  @Override
  public List<FieldSchema> getCols() {
    copyCols();
    return super.getCols();
  }

  @Override
  public void setCols(List<FieldSchema> cols) {
    colsCopied = true;
    super.setCols(cols);
  }

  @Override
  public void unsetCols() {
    colsCopied = true;
    super.unsetCols();
  }

  @Override
  public Iterator<FieldSchema> getColsIterator() {
    copyCols();
    return super.getColsIterator();
  }

  private void copyCols() {
    if (!colsCopied) {
      colsCopied = true;
      if (super.getCols() != null) {
        List<FieldSchema> cols = new ArrayList<FieldSchema>(super.getColsSize());
        for (FieldSchema fs : super.getCols()) cols.add(new FieldSchema(fs));
        super.setCols(cols);
      }
    }
  }

  @Override
  public SerDeInfo getSerdeInfo() {
    copySerde();
    return super.getSerdeInfo();
  }

  @Override
  public void setSerdeInfo(SerDeInfo serdeInfo) {
    serdeCopied = true;
    super.setSerdeInfo(serdeInfo);
  }

  @Override
  public void unsetSerdeInfo() {
    serdeCopied = true;
    super.unsetSerdeInfo();
  }

  private void copySerde() {
    if (!serdeCopied) {
      serdeCopied = true;
      if (super.getSerdeInfo() != null) super.setSerdeInfo(new SerDeInfo(super.getSerdeInfo()));
    }
  }

  @Override
  public void addToBucketCols(String bucket) {
    copyBucketCols();
    super.addToBucketCols(bucket);
  }

  @Override
  public List<String> getBucketCols() {
    copyBucketCols();
    return super.getBucketCols();
  }

  @Override
  public void setBucketCols(List<String> buckets) {
    bucketsCopied = true;
    super.setBucketCols(buckets);
  }

  @Override
  public void unsetBucketCols() {
    bucketsCopied = true;
    super.unsetBucketCols();
  }

  @Override
  public Iterator<String> getBucketColsIterator() {
    copyBucketCols();
    return super.getBucketColsIterator();
  }

  private void copyBucketCols() {
    if (!bucketsCopied) {
      bucketsCopied = true;
      if (super.getBucketCols() != null) {
        List<String> buckets = new ArrayList<String>(super.getBucketColsSize());
        for (String bucket : super.getBucketCols()) buckets.add(bucket);
        super.setBucketCols(buckets);
      }
    }
  }

  @Override
  public void addToSortCols(Order sort) {
    copySort();
    super.addToSortCols(sort);
  }

  @Override
  public List<Order> getSortCols() {
    copySort();
    return super.getSortCols();
  }

  @Override
  public void setSortCols(List<Order> sorts) {
    sortCopied = true;
    super.setSortCols(sorts);
  }

  @Override
  public void unsetSortCols() {
    sortCopied = true;
    super.unsetSortCols();
  }

  @Override
  public Iterator<Order> getSortColsIterator() {
    copySort();
    return super.getSortColsIterator();
  }

  private void copySort() {
    if (!sortCopied) {
      sortCopied = true;
      if (super.getSortCols() != null) {
        List<Order> sortCols = new ArrayList<Order>(super.getSortColsSize());
        for (Order sortCol : super.getSortCols()) sortCols.add(new Order(sortCol));
        super.setSortCols(sortCols);
      }
    }
  }

  @Override
  public SkewedInfo getSkewedInfo() {
    copySkewed();
    return super.getSkewedInfo();
  }

  @Override
  public void setSkewedInfo(SkewedInfo skewedInfo) {
    skewedCopied = true;
    super.setSkewedInfo(skewedInfo);
  }

  @Override
  public void unsetSkewedInfo() {
    skewedCopied = true;
    super.unsetSkewedInfo();
  }

  private void copySkewed() {
    if (!skewedCopied) {
      skewedCopied = true;
      if (super.getSkewedInfo() != null) super.setSkewedInfo(new SkewedInfo(super.getSkewedInfo()));
    }
  }
}
