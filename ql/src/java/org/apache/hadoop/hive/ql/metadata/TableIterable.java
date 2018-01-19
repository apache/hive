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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;

/**
 * Use this to get Table objects for a table list. It provides an iterator to
 * on the resulting Table objects. It batches the calls to
 * IMetaStoreClient.getTableObjectsByName to avoid OOM issues in HS2 (with
 * embedded metastore) or MetaStore server (if HS2 is using remote metastore).
 *
 */
public class TableIterable implements Iterable<Table> {

  @Override
  public Iterator<Table> iterator() {
    return new Iterator<Table>() {

      private final Iterator<String> tableNamesIter = tableNames.iterator();
      private Iterator<org.apache.hadoop.hive.metastore.api.Table> batchIter = null;

      @Override
      public boolean hasNext() {
        return ((batchIter != null) && batchIter.hasNext()) || tableNamesIter.hasNext();
      }

      @Override
      public Table next() {
        if ((batchIter == null) || !batchIter.hasNext()) {
          getNextBatch();
        }
        return batchIter.next();
      }

      private void getNextBatch() {
        // get next batch of table names in this list
        List<String> nameBatch = new ArrayList<String>();
        int batch_counter = 0;
        while (batch_counter < batch_size && tableNamesIter.hasNext()) {
          nameBatch.add(tableNamesIter.next());
          batch_counter++;
        }
        // get the Table objects for this batch of table names and get iterator
        // on it
        try {
          try {
            batchIter = msc.getTableObjectsByName(dbname, nameBatch).iterator();
          } catch (TException e) {
            throw new HiveException(e);
          }
        } catch (HiveException e) {
          throw new RuntimeException(e);
        }
      }

      @Override
      public void remove() {
        throw new IllegalStateException(
            "TableIterable is a read-only iterable and remove() is unsupported");
      }
    };
  }

  private final IMetaStoreClient msc;
  private final String dbname;
  private final List<String> tableNames;
  private final int batch_size;

  /**
   * Primary constructor that fetches all tables in a given msc, given a Hive
   * object,a db name and a table name list
   */
  public TableIterable(IMetaStoreClient msc, String dbname, List<String> tableNames, int batch_size)
      throws TException {
    this.msc = msc;
    this.dbname = dbname;
    this.tableNames = tableNames;
    this.batch_size = batch_size;
  }

}
