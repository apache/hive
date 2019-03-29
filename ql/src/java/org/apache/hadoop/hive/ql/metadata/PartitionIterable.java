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

package org.apache.hadoop.hive.ql.metadata;

import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * PartitionIterable - effectively a lazy Iterable&lt;Partition&gt;
 *
 * Sometimes, we have a need for iterating through a list of partitions,
 * but the list of partitions can be too big to fetch as a single object.
 * Thus, the goal of PartitionIterable is to act as an Iterable&lt;Partition&gt;
 * while lazily fetching each relevant partition, one after the other as
 * independent metadata calls.
 *
 * It is very likely that any calls to PartitionIterable are going to result
 * in a large number of calls, so use sparingly only when the memory cost
 * of fetching all the partitions in one shot is too prohibitive.
 *
 * This is still pretty costly in that it would retain a list of partition
 * names, but that should be far less expensive than the entire partition
 * objects.
 *
 * Note that remove() is an illegal call on this, and will result in an
 * IllegalStateException.
 */
public class PartitionIterable implements Iterable<Partition> {
  private static final Logger LOG = LoggerFactory.getLogger(PartitionIterable.class);

  @Override
  public Iterator<Partition> iterator() {
    return new Iterator<Partition>(){

      private boolean initialized = false;
      private Iterator<Partition> ptnsIterator = null;

      private Iterator<String> partitionNamesIter = null;
      private Iterator<Partition> batchIter = null;

      private Iterator<Partition> partitionFilterIter;

      private void initialize(){
        if(!initialized){
          if (currType == Type.LIST_PROVIDED){
            ptnsIterator = ptnsProvided.iterator();
          } else if (currType == Type.FILTER_PROVIDED) {
            try {
              List<Partition> result = new ArrayList<>();
              boolean hasUnknown = db.getPartitionsByExpr(table,
                      (ExprNodeGenericFuncDesc)partitionFilter, db.getConf(), result);
              if (hasUnknown) {
                throw new SemanticException(
                        "Unexpected unknown partitions for " + partitionFilter.getExprString());
              }
              partitionFilterIter = result.iterator();
            } catch (Exception e) {
              LOG.error("Failed to extract partitions for " + table.getDbName() + "." + table.getTableName(), e);
              throw new RuntimeException(e.getMessage());
            }
          } else {
            partitionNamesIter = partitionNames.iterator();
          }
          initialized = true;
        }
      }

      @Override
      public boolean hasNext() {
        initialize();
        if (currType == Type.LIST_PROVIDED){
          return ptnsIterator.hasNext();
        } else if (currType == Type.FILTER_PROVIDED) {
          return partitionFilterIter == null ? false : partitionFilterIter.hasNext();
        } else {
          return ((batchIter != null) && batchIter.hasNext()) || partitionNamesIter.hasNext();
        }
      }

      @Override
      public Partition next() {
        initialize();
        if (currType == Type.LIST_PROVIDED){
          return ptnsIterator.next();
        }

        if (currType == Type.FILTER_PROVIDED) {
          return partitionFilterIter.next();
        }

        if ((batchIter == null) || !batchIter.hasNext()){
          getNextBatch();
        }

        return batchIter.next();
      }

      private void getNextBatch() {
        int batchCounter = 0;
        List<String> nameBatch = new ArrayList<String>();
        while (batchCounter < batchSize && partitionNamesIter.hasNext()){
          nameBatch.add(partitionNamesIter.next());
          batchCounter++;
        }
        try {
          batchIter = db.getPartitionsByNames(table, nameBatch, getColStats).iterator();
        } catch (HiveException e) {
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
    LAZY_FETCH_PARTITIONS, // Where we want to fetch Partitions lazily when they're needed.
    FILTER_PROVIDED // for partition level replication.
  };

  final Type currType;

  // used for LIST_PROVIDED cases
  private Collection<Partition> ptnsProvided = null;

  // used for LAZY_FETCH_PARTITIONS cases
  private Hive db = null; // Assumes one instance of this + single-threaded compilation for each query.
  private Table table = null;
  private Map<String, String> partialPartitionSpec = null;
  private List<String> partitionNames = null;
  private int batchSize;
  private boolean getColStats = false;
  ExprNodeDesc partitionFilter;

  public PartitionIterable(Hive db, Table tbl, ExprNodeDesc filter) {
    this.currType = Type.FILTER_PROVIDED;
    this.table = tbl;
    this.partitionFilter = filter;
    this.db = db;
  }

  /**
   * Dummy constructor, which simply acts as an iterator on an already-present
   * list of partitions, allows for easy drop-in replacement for other methods
   * that already have a List&lt;Partition&gt;
   */
  public PartitionIterable(Collection<Partition> ptnsProvided){
    this.currType = Type.LIST_PROVIDED;
    this.ptnsProvided = ptnsProvided;
  }

  /**
   * Primary constructor that fetches all partitions in a given table, given
   * a Hive object and a table object, and a partial partition spec.
   */
  public PartitionIterable(Hive db, Table table, Map<String, String> partialPartitionSpec,
                           int batchSize) throws HiveException {
    this(db, table, partialPartitionSpec, batchSize, false);
  }

  /**
   * Primary constructor that fetches all partitions in a given table, given
   * a Hive object and a table object, and a partial partition spec.
   */
  public PartitionIterable(Hive db, Table table, Map<String, String> partialPartitionSpec,
                           int batchSize, boolean getColStats) throws HiveException {
    this.currType = Type.LAZY_FETCH_PARTITIONS;
    this.db = db;
    this.table = table;
    this.partialPartitionSpec = partialPartitionSpec;
    this.batchSize = batchSize;
    this.getColStats = getColStats;

    if (this.partialPartitionSpec == null){
      partitionNames = db.getPartitionNames(
          table.getDbName(),table.getTableName(), (short) -1);
    } else {
      partitionNames = db.getPartitionNames(
          table.getDbName(),table.getTableName(),partialPartitionSpec,(short)-1);
    }
  }

}
