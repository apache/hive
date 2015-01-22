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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.metadata;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;


/**
 * PartitionIterable - effectively a lazy Iterable<Partition>
 *
 * Sometimes, we have a need for iterating through a list of partitions,
 * but the list of partitions can be too big to fetch as a single object.
 * Thus, the goal of PartitionIterable is to act as an Iterable<Partition>
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

  @Override
  public Iterator<Partition> iterator() {
    return new Iterator<Partition>(){

      private boolean initialized = false;
      private Iterator<Partition> ptnsIterator = null;

      private Iterator<String> partitionNamesIter = null;
      private Iterator<Partition> batchIter = null;

      private void initialize(){
        if(!initialized){
          if (currType == Type.LIST_PROVIDED){
            ptnsIterator = ptnsProvided.iterator();
          } else {
            partitionNamesIter = partitionNames.iterator();
          }
          initialized = true;
        }
      }

      public boolean hasNext() {
        initialize();
        if (currType == Type.LIST_PROVIDED){
          return ptnsIterator.hasNext();
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

        if ((batchIter == null) || !batchIter.hasNext()){
          getNextBatch();
        }

        return batchIter.next();
      }

      private void getNextBatch() {
        int batch_counter = 0;
        List<String> nameBatch = new ArrayList<String>();
        while (batch_counter < batch_size && partitionNamesIter.hasNext()){
          nameBatch.add(partitionNamesIter.next());
          batch_counter++;
        }
        try {
          batchIter = db.getPartitionsByNames(table,nameBatch).iterator();
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
    LAZY_FETCH_PARTITIONS // Where we want to fetch Partitions lazily when they're needed.
  };

  final Type currType;

  // used for LIST_PROVIDED cases
  private List<Partition> ptnsProvided = null;

  // used for LAZY_FETCH_PARTITIONS cases
  private Hive db = null;
  private Table table = null;
  private Map<String, String> partialPartitionSpec = null;
  private List<String> partitionNames = null;
  private int batch_size;

  /**
   * Dummy constructor, which simply acts as an iterator on an already-present
   * list of partitions, allows for easy drop-in replacement for other methods
   * that already have a List<Partition>
   */
  public PartitionIterable(List<Partition> ptnsProvided){
    this.currType = Type.LIST_PROVIDED;
    this.ptnsProvided = ptnsProvided;
  }

  /**
   * Primary constructor that fetches all partitions in a given table, given
   * a Hive object and a table object, and a partial partition spec.
   */
  public PartitionIterable(Hive db, Table table, Map<String, String> partialPartitionSpec,
      int batch_size) throws HiveException {
    this.currType = Type.LAZY_FETCH_PARTITIONS;
    this.db = db;
    this.table = table;
    this.partialPartitionSpec = partialPartitionSpec;
    this.batch_size = batch_size;

    if (this.partialPartitionSpec == null){
      partitionNames = db.getPartitionNames(
          table.getDbName(),table.getTableName(), (short) -1);
    } else {
      partitionNames = db.getPartitionNames(
          table.getDbName(),table.getTableName(),partialPartitionSpec,(short)-1);
    }
  }

}
