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

import java.util.ArrayList;
import java.util.Collection;
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
  private StorageDescriptor shared;
  private boolean copied = false;
  private CopyOnWriteColList colList = null;
  private CopyOnWriteOrderList orderList = null;
  private CopyOnWriteBucketList bucketList = null;

  SharedStorageDescriptor() {
  }

  public SharedStorageDescriptor(SharedStorageDescriptor that) {
    this.setLocation(that.getLocation());
    this.setParameters(that.getParameters());
    this.shared = that.shared;
  }

  @Override
  public StorageDescriptor deepCopy() {
    return new SharedStorageDescriptor(this);
  }

  @Override
  public boolean isSetCols() {
    return shared.isSetCols();
  }

  @Override
  public List<FieldSchema> getCols() {
    return copied ? shared.getCols() : (
        shared.getCols() == null ? null : copyCols(shared.getCols()));
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
  public void setCols(List<FieldSchema> cols) {
    copyOnWrite();
    shared.setCols(cols);
  }

  @Override
  public void addToCols(FieldSchema fs) {
    copyOnWrite();
    shared.addToCols(fs);
  }

  @Override
  public void unsetCols() {
    copyOnWrite();
    shared.unsetCols();
  }

  @Override
  public boolean isSetInputFormat() {
    return shared.isSetInputFormat();
  }

  @Override
  public String getInputFormat() {
    return shared.getInputFormat();
  }

  @Override
  public void setInputFormat(String inputFormat) {
    copyOnWrite();
    shared.setInputFormat(inputFormat);
  }

  @Override
  public void unsetInputFormat() {
    copyOnWrite();
    shared.unsetInputFormat();
  }

  @Override
  public boolean isSetOutputFormat() {
    return shared.isSetOutputFormat();
  }

  @Override
  public String getOutputFormat() {
    return shared.getOutputFormat();
  }

  @Override
  public void setOutputFormat(String outputFormat) {
    copyOnWrite();
    shared.setOutputFormat(outputFormat);
  }

  @Override
  public void unsetOutputFormat() {
    copyOnWrite();
    shared.unsetOutputFormat();
  }

  @Override
  public boolean isSetCompressed() {
    return shared.isSetCompressed();
  }

  @Override
  public boolean isCompressed() {
    return shared.isCompressed();
  }

  @Override
  public void setCompressed(boolean isCompressed) {
    copyOnWrite();
    shared.setCompressed(isCompressed);
  }

  @Override
  public void unsetCompressed() {
    copyOnWrite();
    shared.unsetCompressed();
  }

  @Override
  public boolean isSetNumBuckets() {
    return shared.isSetNumBuckets();
  }

  @Override
  public int getNumBuckets() {
    return shared.getNumBuckets();
  }

  @Override
  public void setNumBuckets(int numBuckets) {
    copyOnWrite();
    shared.setNumBuckets(numBuckets);
  }

  @Override
  public void unsetNumBuckets() {
    copyOnWrite();
    shared.unsetNumBuckets();
  }

  @Override
  public boolean isSetSerdeInfo() {
    return shared.isSetSerdeInfo();
  }

  @Override
  public SerDeInfo getSerdeInfo() {
    return copied ? shared.getSerdeInfo() : (
        shared.getSerdeInfo() == null ? null : new SerDeInfoWrapper(shared.getSerdeInfo()));
  }

  @Override
  public void setSerdeInfo(SerDeInfo serdeInfo) {
    copyOnWrite();
    shared.setSerdeInfo(serdeInfo);
  }

  @Override
  public void unsetSerdeInfo() {
    copyOnWrite();
    shared.unsetSerdeInfo();
  }

  @Override
  public boolean isSetBucketCols() {
    return shared.isSetBucketCols();
  }

  @Override
  public List<String> getBucketCols() {
    return copied ? shared.getBucketCols() : (
        shared.getBucketCols() == null ? null : copyBucketCols(shared.getBucketCols()));
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
  public void setBucketCols(List<String> bucketCols) {
    copyOnWrite();
    shared.setBucketCols(bucketCols);
  }

  @Override
  public void addToBucketCols(String bucketCol) {
    copyOnWrite();
    shared.addToBucketCols(bucketCol);
  }

  @Override
  public void unsetBucketCols() {
    copyOnWrite();
    shared.unsetBucketCols();
  }

  @Override
  public boolean isSetSortCols() {
    return shared.isSetSortCols();
  }

  @Override
  public List<Order> getSortCols() {
    return copied ? shared.getSortCols() : (
        shared.getSortCols() == null ? null : copySort(shared.getSortCols()));
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
  public void setSortCols(List<Order> sortCols) {
    copyOnWrite();
    shared.setSortCols(sortCols);
  }

  @Override
  public void addToSortCols(Order sortCol) {
    copyOnWrite();
    shared.addToSortCols(sortCol);
  }

  @Override
  public void unsetSortCols() {
    copyOnWrite();
    shared.unsetSortCols();
  }

  @Override
  public boolean isSetSkewedInfo() {
    return shared.isSetSkewedInfo();
  }

  @Override
  public SkewedInfo getSkewedInfo() {
    return copied ? shared.getSkewedInfo() : (
        shared.getSkewedInfo() == null ? null : new SkewWrapper(shared.getSkewedInfo()));
  }

  @Override
  public void setSkewedInfo(SkewedInfo skewedInfo) {
    copyOnWrite();
    shared.setSkewedInfo(skewedInfo);
  }

  @Override
  public void unsetSkewedInfo() {
    copyOnWrite();
    shared.unsetSkewedInfo();
  }

  @Override
  public boolean isSetStoredAsSubDirectories() {
    return shared.isSetStoredAsSubDirectories();
  }

  @Override
  public boolean isStoredAsSubDirectories() {
    return shared.isStoredAsSubDirectories();
  }

  @Override
  public void setStoredAsSubDirectories(boolean sasd) {
    copyOnWrite();
    shared.setStoredAsSubDirectories(sasd);
  }

  @Override
  public void unsetStoredAsSubDirectories() {
    copyOnWrite();
    shared.unsetStoredAsSubDirectories();
  }

  void setShared(StorageDescriptor sd) {
    shared = sd;
  }

  StorageDescriptor getShared() {
    return shared;
  }

  private void copyOnWrite() {
    if (!copied) {
      shared = new StorageDescriptor(shared);
      copied = true;
    }
  }

  private class SerDeInfoWrapper extends SerDeInfo {

    SerDeInfoWrapper(SerDeInfo serde) {
      super(serde);
    }

    @Override
    public void setName(String name) {
      copyOnWrite();
      shared.getSerdeInfo().setName(name);
    }

    @Override
    public void unsetName() {
      copyOnWrite();
      shared.getSerdeInfo().unsetName();
    }

    @Override
    public void setSerializationLib(String lib) {
      copyOnWrite();
      shared.getSerdeInfo().setSerializationLib(lib);
    }

    @Override
    public void unsetSerializationLib() {
      copyOnWrite();
      shared.getSerdeInfo().unsetSerializationLib();
    }

    @Override
    public void setParameters(Map<String, String> parameters) {
      copyOnWrite();
      shared.getSerdeInfo().setParameters(parameters);
    }

    @Override
    public void unsetParameters() {
      copyOnWrite();
      shared.getSerdeInfo().unsetParameters();
    }

    @Override
    public void putToParameters(String key, String value) {
      copyOnWrite();
      shared.getSerdeInfo().putToParameters(key, value);
    }
  }

  private class SkewWrapper extends SkewedInfo {
    SkewWrapper(SkewedInfo skew) {
      super(skew);
    }

    @Override
    public void setSkewedColNames(List<String> skewedColNames) {
      copyOnWrite();
      shared.getSkewedInfo().setSkewedColNames(skewedColNames);
    }

    @Override
    public void unsetSkewedColNames() {
      copyOnWrite();
      shared.getSkewedInfo().unsetSkewedColNames();
    }

    @Override
    public void addToSkewedColNames(String skewCol) {
      copyOnWrite();
      shared.getSkewedInfo().addToSkewedColNames(skewCol);
    }

    @Override
    public void setSkewedColValues(List<List<String>> skewedColValues) {
      copyOnWrite();
      shared.getSkewedInfo().setSkewedColValues(skewedColValues);
    }

    @Override
    public void unsetSkewedColValues() {
      copyOnWrite();
      shared.getSkewedInfo().unsetSkewedColValues();
    }

    @Override
    public void addToSkewedColValues(List<String> skewedColValue) {
      copyOnWrite();
      shared.getSkewedInfo().addToSkewedColValues(skewedColValue);
    }

    @Override
    public void setSkewedColValueLocationMaps(Map<List<String>, String> maps) {
      copyOnWrite();
      shared.getSkewedInfo().setSkewedColValueLocationMaps(maps);
    }

    @Override
    public void unsetSkewedColValueLocationMaps() {
      copyOnWrite();
      shared.getSkewedInfo().unsetSkewedColValueLocationMaps();
    }

    @Override
    public void putToSkewedColValueLocationMaps(List<String> key, String value) {
      copyOnWrite();
      shared.getSkewedInfo().putToSkewedColValueLocationMaps(key, value);
    }
  }

  private CopyOnWriteOrderList copySort(List<Order> sort) {
    if (orderList == null) {
      orderList = new CopyOnWriteOrderList(sort.size());
      for (int i = 0; i < sort.size(); i++) {
        orderList.secretAdd(new OrderWrapper(i, sort.get(i)));
      }
    }
    return orderList;
  }

  private class CopyOnWriteOrderList extends ArrayList<Order> {

    CopyOnWriteOrderList(int size) {
      super(size);
    }

    private void secretAdd(OrderWrapper order) {
      super.add(order);
    }

    @Override
    public boolean add(Order t) {
      copyOnWrite();
      return shared.getSortCols().add(t);
    }

    @Override
    public boolean remove(Object o) {
      copyOnWrite();
      return shared.getSortCols().remove(o);
    }

    @Override
    public boolean addAll(Collection<? extends Order> c) {
      copyOnWrite();
      return shared.getSortCols().addAll(c);
    }

    @Override
    public boolean addAll(int index, Collection<? extends Order> c) {
      copyOnWrite();
      return shared.getSortCols().addAll(c);
    }

    @Override
    public boolean removeAll(Collection<?> c) {
      copyOnWrite();
      return shared.getSortCols().removeAll(c);
    }

    @Override
    public boolean retainAll(Collection<?> c) {
      copyOnWrite();
      return shared.getSortCols().retainAll(c);
    }

    @Override
    public void clear() {
      copyOnWrite();
      shared.getSortCols().clear();
    }

    @Override
    public Order set(int index, Order element) {
      copyOnWrite();
      return shared.getSortCols().set(index, element);
    }

    @Override
    public void add(int index, Order element) {
      copyOnWrite();
      shared.getSortCols().add(index, element);
    }

    @Override
    public Order remove(int index) {
      copyOnWrite();
      return shared.getSortCols().remove(index);
    }
  }

  private class OrderWrapper extends Order {
    final private int pos;

    OrderWrapper(int pos, Order order) {
      super(order);
      this.pos = pos;
    }

    @Override
    public void setCol(String col) {
      copyOnWrite();
      shared.getSortCols().get(pos).setCol(col);
    }

    @Override
    public void unsetCol() {
      copyOnWrite();
      shared.getSortCols().get(pos).unsetCol();
    }

    @Override
    public void setOrder(int order) {
      copyOnWrite();
      shared.getSortCols().get(pos).setOrder(order);
    }

    @Override
    public void unsetOrder() {
      copyOnWrite();
      shared.getSortCols().get(pos).unsetOrder();
    }
  }

  private CopyOnWriteColList copyCols(List<FieldSchema> cols) {
    if (colList == null) {
      colList = new CopyOnWriteColList(cols.size());
      for (int i = 0; i < cols.size(); i++) {
        colList.secretAdd(new FieldSchemaWrapper(i, cols.get(i)));
      }
    }
    return colList;
  }

  private class CopyOnWriteColList extends ArrayList<FieldSchema> {

    CopyOnWriteColList(int size) {
      super(size);
    }

    private void secretAdd(FieldSchemaWrapper col) {
      super.add(col);
    }

    @Override
    public boolean add(FieldSchema t) {
      copyOnWrite();
      return shared.getCols().add(t);
    }

    @Override
    public boolean remove(Object o) {
      copyOnWrite();
      return shared.getCols().remove(o);
    }

    @Override
    public boolean addAll(Collection<? extends FieldSchema> c) {
      copyOnWrite();
      return shared.getCols().addAll(c);
    }

    @Override
    public boolean addAll(int index, Collection<? extends FieldSchema> c) {
      copyOnWrite();
      return shared.getCols().addAll(c);
    }

    @Override
    public boolean removeAll(Collection<?> c) {
      copyOnWrite();
      return shared.getCols().removeAll(c);
    }

    @Override
    public boolean retainAll(Collection<?> c) {
      copyOnWrite();
      return shared.getCols().retainAll(c);
    }

    @Override
    public void clear() {
      copyOnWrite();
      shared.getCols().clear();
    }

    @Override
    public FieldSchema set(int index, FieldSchema element) {
      copyOnWrite();
      return shared.getCols().set(index, element);
    }

    @Override
    public void add(int index, FieldSchema element) {
      copyOnWrite();
      shared.getCols().add(index, element);
    }

    @Override
    public FieldSchema remove(int index) {
      copyOnWrite();
      return shared.getCols().remove(index);
    }
  }

  private class FieldSchemaWrapper extends FieldSchema {
    final private int pos;

    FieldSchemaWrapper(int pos, FieldSchema col) {
      super(col);
      this.pos = pos;
    }

    @Override
    public void setName(String name) {
      copyOnWrite();
      shared.getCols().get(pos).setName(name);
    }

    @Override
    public void unsetName() {
      copyOnWrite();
      shared.getCols().get(pos).unsetName();
    }

    @Override
    public void setType(String type) {
      copyOnWrite();
      shared.getCols().get(pos).setType(type);
    }

    @Override
    public void unsetType() {
      copyOnWrite();
      shared.getCols().get(pos).unsetType();
    }

    @Override
    public void setComment(String comment) {
      copyOnWrite();
      shared.getCols().get(pos).setComment(comment);
    }

    @Override
    public void unsetComment() {
      copyOnWrite();
      shared.getCols().get(pos).unsetComment();
    }
  }

  private CopyOnWriteBucketList copyBucketCols(List<String> cols) {
    if (bucketList == null) {
      bucketList = new CopyOnWriteBucketList(cols);
    }
    return bucketList;
  }

  private class CopyOnWriteBucketList extends ArrayList<String> {

    CopyOnWriteBucketList(Collection<String> c) {
      super(c);
    }

    private void secretAdd(String col) {
      super.add(col);
    }

    @Override
    public boolean add(String t) {
      copyOnWrite();
      return shared.getBucketCols().add(t);
    }

    @Override
    public boolean remove(Object o) {
      copyOnWrite();
      return shared.getBucketCols().remove(o);
    }

    @Override
    public boolean addAll(Collection<? extends String> c) {
      copyOnWrite();
      return shared.getBucketCols().addAll(c);
    }

    @Override
    public boolean addAll(int index, Collection<? extends String> c) {
      copyOnWrite();
      return shared.getBucketCols().addAll(c);
    }

    @Override
    public boolean removeAll(Collection<?> c) {
      copyOnWrite();
      return shared.getBucketCols().removeAll(c);
    }

    @Override
    public boolean retainAll(Collection<?> c) {
      copyOnWrite();
      return shared.getBucketCols().retainAll(c);
    }

    @Override
    public void clear() {
      copyOnWrite();
      shared.getBucketCols().clear();
    }

    @Override
    public String set(int index, String element) {
      copyOnWrite();
      return shared.getBucketCols().set(index, element);
    }

    @Override
    public void add(int index, String element) {
      copyOnWrite();
      shared.getBucketCols().add(index, element);
    }

    @Override
    public String remove(int index) {
      copyOnWrite();
      return shared.getBucketCols().remove(index);
    }
  }

}
