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

package org.apache.hadoop.hive.metastore;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hive.metastore.api.GetPartitionsByNamesRequest;
import org.apache.hadoop.hive.metastore.api.GetPartitionsFilterSpec;
import org.apache.hadoop.hive.metastore.api.GetPartitionsRequest;
import org.apache.hadoop.hive.metastore.api.MetastoreException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PartitionFilterMode;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.utils.MetaStoreServerUtils;

import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.convertToGetPartitionsByNamesRequest;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.prependCatalogToDbName;


/**
 * PartitionIterable - effectively a lazy Iterable&lt;Partition&gt;
 * Sometimes, we have a need for iterating through a list of partitions,
 * but the list of partitions can be too big to fetch as a single object.
 * Thus, the goal of PartitionIterable is to act as an Iterable&lt;Partition&gt;
 * while lazily fetching each relevant partition, one after the other as
 * independent metadata calls.
 * It is very likely that any calls to PartitionIterable are going to result
 * in a large number of calls, so use sparingly only when the memory cost
 * of fetching all the partitions in one shot is too prohibitive.
 * This is still pretty costly in that it would retain a list of partition
 * names, but that should be far less expensive than the entire partition
 * objects.
 * Note that remove() is an illegal call on this, and will result in an
 * IllegalStateException.
 */
public class PartitionIterable implements Iterable<Partition> {

  @Override
  public Iterator<Partition> iterator() {
    return new Iterator<Partition>() {

      private boolean initialized = false;
      private Iterator<Partition> ptnsIterator = null;

      private Iterator<String> partitionNamesIter = null;
      private Iterator<Partition> batchIter = null;

      private void initialize() {
        if (!initialized) {
          if (currType == Type.LIST_PROVIDED) {
            ptnsIterator = ptnsProvided.iterator();
          } else {
            partitionNamesIter = partitionNames.iterator();
          }
          initialized = true;
        }
      }

      @Override
      public boolean hasNext() {
        initialize();
        if (currType == Type.LIST_PROVIDED) {
          return ptnsIterator.hasNext();
        } else {
          return ((batchIter != null) && batchIter.hasNext()) || partitionNamesIter.hasNext();
        }
      }

      @Override
      public Partition next() {
        initialize();
        if (currType == Type.LIST_PROVIDED) {
          return ptnsIterator.next();
        }

        if ((batchIter == null) || !batchIter.hasNext()) {
          getNextBatch();
        }

        return batchIter.next();
      }

      private void getNextBatch() {
        int batch_counter = 0;
        List<String> nameBatch = new ArrayList<String>();
        while (batch_counter < batch_size && partitionNamesIter.hasNext()) {
          nameBatch.add(partitionNamesIter.next());
          batch_counter++;
        }
        try {
          if (request != null) {
            GetPartitionsFilterSpec getPartitionsFilterSpec = new GetPartitionsFilterSpec();
            getPartitionsFilterSpec.setFilterMode(PartitionFilterMode.BY_NAMES);
            getPartitionsFilterSpec.setFilters(nameBatch);
            request.setFilterSpec(getPartitionsFilterSpec);
            batchIter = MetaStoreServerUtils.getPartitionsByProjectSpec(msc, request).iterator();
          } else {
            String dbName = prependCatalogToDbName(table.getCatName(), table.getDbName(), null);
            GetPartitionsByNamesRequest req = convertToGetPartitionsByNamesRequest(dbName, table.getTableName(),
                nameBatch);
            batchIter = msc.getPartitionsByNames(req).getPartitionsIterator();
          }
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }

      @Override
      public void remove() {
        throw new IllegalStateException(
          "PartitionIterable is a read-only iterable and remove() is unsupported");
      }
    };
  }

  enum Type {
    LIST_PROVIDED,  // Where a List<Partitions is already provided
    LAZY_FETCH_PARTITIONS // Where we want to fetch Partitions lazily when they're needed.
  }

  private final Type currType;

  // used for LIST_PROVIDED cases
  private Collection<Partition> ptnsProvided = null;

  // used for LAZY_FETCH_PARTITIONS cases
  private IMetaStoreClient msc = null; // Assumes one instance of this + single-threaded compilation for each query.
  private Table table = null;
  private List<String> partitionNames = null;
  private GetPartitionsRequest request = null;
  private int batch_size;

  /**
   * Dummy constructor, which simply acts as an iterator on an already-present
   * list of partitions, allows for easy drop-in replacement for other methods
   * that already have a List&lt;Partition&gt;
   */
  public PartitionIterable(Collection<Partition> ptnsProvided) {
    this.currType = Type.LIST_PROVIDED;
    this.ptnsProvided = ptnsProvided;
  }

  /**
   * Primary constructor that fetches all partitions in a given table, given
   * a Hive object and a table object, and a partial partition spec.
   */
  public PartitionIterable(IMetaStoreClient msc, Table table, int batch_size) throws MetastoreException {
    if (batch_size < 1) {
      throw new MetastoreException("Invalid batch size for partition iterable. Please use a batch size greater than 0");
    }
    this.currType = Type.LAZY_FETCH_PARTITIONS;
    this.msc = msc;
    this.table = table;
    this.batch_size = batch_size;
    partitionNames = getPartitionNames(msc, table.getCatName(), table.getDbName(), table.getTableName(), (short) -1);
  }

  public List<String> getPartitionNames(IMetaStoreClient msc, String catName, String dbName, String tblName, short max)
    throws MetastoreException {
    try {
      return msc.listPartitionNames(catName, dbName, tblName, max);
    } catch (Exception e) {
      throw new MetastoreException(e);
    }
  }

  public PartitionIterable withProjectSpec(GetPartitionsRequest request) {
    this.request = request;
    return this;
  }
}
