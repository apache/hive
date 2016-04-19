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
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.junit.Test;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.thrift.TException;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
/**
 * Unit tests for TableIterable
 */
public class TestTableIterable  {

  @Test
  public void testNumReturned() throws MetaException, InvalidOperationException, UnknownDBException, TException {
    HiveMetaStoreClient msc = mock(HiveMetaStoreClient.class);


    // create a mocked metastore client that returns 3 table objects every time it is called
    // will use same size for TableIterable batch fetch size
    List<Table> threeTables = Arrays.asList(new Table(), new Table(), new Table());
    when(msc.getTableObjectsByName(anyString(), anyListOf(String.class))).thenReturn(threeTables);

    List<String> tableNames = Arrays.asList("a", "b", "c", "d", "e", "f");
    TableIterable tIterable = new TableIterable(msc, "dummy", tableNames, threeTables.size());
    tIterable.iterator();

    Iterator<Table> tIter = tIterable.iterator();
    int size = 0;
    while(tIter.hasNext()) {
      size++;
      tIter.next();
    }
    assertEquals("Number of table objects returned", size, tableNames.size());

    verify(msc).getTableObjectsByName("dummy", Arrays.asList("a","b","c"));
    verify(msc).getTableObjectsByName("dummy", Arrays.asList("d","e","f"));
    
  }
}
