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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.hive.metastore.metastore;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.metastore.RawStore;
import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.client.builder.GetPartitionsArgs;
import org.apache.hadoop.hive.metastore.metastore.iface.TableStore;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Category(MetastoreUnitTest.class)
public class TestPartitionsIterable {

  @FunctionalInterface
  private interface PartitionFetcher {
    List<Partition> fetch(GetPartitionsArgs args) throws Exception;
  }

  private static final TableName TABLE = new TableName("hive", "default", "t");

  private static Partition mkPart(String id) {
    Partition p = new Partition();
    p.setValues(Collections.singletonList(id));
    return p;
  }

  private static List<String> values(List<Partition> parts) {
    return parts.stream().map(p -> p.getValues().get(0)).collect(Collectors.toList());
  }

  private static List<List<Partition>> toBatches(PartitionsIterable iterable) throws MetaException {
    List<List<Partition>> batches = new ArrayList<>();
    for (List<Partition> batch : iterable) {
      batches.add(batch);
    }
    return batches;
  }

  private static void stubPartitionsByNames(TableStore store, PartitionFetcher fetcher) throws Exception {
    when(store.getPartitionsByNames(eq(TABLE), any(GetPartitionsArgs.class))).thenAnswer(invocation -> {
      GetPartitionsArgs args = invocation.getArgument(1);
      return fetcher.fetch(args);
    });
  }

  @Test
  public void emptyPartNamesYieldsNoBatches() throws Exception {
    TableStore store = mock(TableStore.class);
    PartitionsIterable iterable = PartitionsIterable.create(store, TABLE, 10, Collections.emptyList());
    Iterator<List<Partition>> it = iterable.iterator();
    Assert.assertFalse(it.hasNext());
  }

  @Test
  public void yieldsPartitionBatchesUpToBatchSize() throws Exception {
    TableStore store = mock(TableStore.class);
    stubPartitionsByNames(store, args -> {
      List<Partition> out = new ArrayList<>();
      for (String name : args.getPartNames()) {
        out.add(mkPart(name));
      }
      return out;
    });

    List<String> names = Arrays.asList("p0", "p1", "p2", "p3", "p4");
    List<List<Partition>> batches = toBatches(PartitionsIterable.create(store, TABLE, 2, names));

    Assert.assertEquals(3, batches.size());
    Assert.assertEquals(Arrays.asList("p0", "p1"), values(batches.get(0)));
    Assert.assertEquals(Arrays.asList("p2", "p3"), values(batches.get(1)));
    Assert.assertEquals(Collections.singletonList("p4"), values(batches.get(2)));
  }

  @Test
  public void accumulatesAcrossFetchesWhenMetastoreReturnsFewPartitions() throws Exception {
    TableStore store = mock(TableStore.class);
    AtomicInteger fetchCount = new AtomicInteger();
    stubPartitionsByNames(store, args -> {
      fetchCount.incrementAndGet();
      return Collections.singletonList(mkPart(args.getPartNames().get(0)));
    });

    List<String> names = new ArrayList<>();
    for (int i = 0; i < 16; i++) {
      names.add("a" + i);
    }
    PartitionsIterable iterable = PartitionsIterable.create(store, TABLE, 4, names);
    List<List<Partition>> batches = toBatches(iterable);

    Assert.assertEquals(1, batches.size());
    Assert.assertEquals(4, batches.get(0).size());
    Assert.assertEquals(4, fetchCount.get());
  }

  @Test
  public void splitsSingleFetchWhenItExceedsBatchSize() throws Exception {
    TableStore store = mock(TableStore.class);
    stubPartitionsByNames(store, args -> {
      List<Partition> out = new ArrayList<>();
      for (String name : args.getPartNames()) {
        out.add(mkPart(name));
        out.add(mkPart(name + "_dup"));
      }
      return out;
    });

    List<String> names = Arrays.asList("p0", "p1");
    List<List<Partition>> batches = toBatches(PartitionsIterable.create(store, TABLE, 2, names));

    Assert.assertEquals(2, batches.size());
    Assert.assertEquals(2, batches.get(0).size());
    Assert.assertEquals(2, batches.get(1).size());
  }

  @Test
  public void continuesPastEmptyMetastoreResponses() throws Exception {
    TableStore store = mock(TableStore.class);
    stubPartitionsByNames(store, args -> {
      if (args.getPartNames().stream().allMatch(n -> n.startsWith("miss"))) {
        return Collections.emptyList();
      }
      return args.getPartNames().stream().map(TestPartitionsIterable::mkPart).collect(Collectors.toList());
    });

    List<String> names = Arrays.asList("miss0", "miss1", "ok0", "ok1");
    List<List<Partition>> batches = toBatches(PartitionsIterable.create(store, TABLE, 2, names));

    Assert.assertEquals(1, batches.size());
    Assert.assertEquals(Arrays.asList("ok0", "ok1"), values(batches.get(0)));
  }

  @Test
  public void nullMetastoreResponseTreatedAsEmpty() throws Exception {
    TableStore store = mock(TableStore.class);
    when(store.getPartitionsByNames(eq(TABLE), any(GetPartitionsArgs.class))).thenReturn(null);

    List<List<Partition>> batches = toBatches(
        PartitionsIterable.create(store, TABLE, 2, Arrays.asList("x", "y")));

    Assert.assertEquals(1, batches.size());
    Assert.assertTrue(batches.get(0).isEmpty());
  }

  @Test
  public void removeUnsupported() throws Exception {
    TableStore store = mock(TableStore.class);
    when(store.getPartitionsByNames(eq(TABLE), any(GetPartitionsArgs.class)))
        .thenReturn(Collections.singletonList(mkPart("p0")));

    Iterator<List<Partition>> it = PartitionsIterable.create(store, TABLE, 10,
        Collections.singletonList("p0")).iterator();
    it.next();
    try {
      it.remove();
      Assert.fail("Expected IllegalStateException");
    } catch (IllegalStateException expected) {
      // expected
    }
  }

  @Test
  public void nextThrowsWhenExhausted() throws Exception {
    TableStore store = mock(TableStore.class);
    Iterator<List<Partition>> it = PartitionsIterable.create(store, TABLE, 10,
        Collections.emptyList()).iterator();
    try {
      it.next();
      Assert.fail("Expected NoSuchElementException");
    } catch (NoSuchElementException expected) {
      // expected
    }
  }

  @Test
  public void createFromRawStoreListsNames() throws Exception {
    RawStore rawStore = mock(RawStore.class);
    TableStore tableStore = mock(TableStore.class);
    when(rawStore.listPartitionNames("hive", "default", "t", (short) -1))
        .thenReturn(Arrays.asList("n0", "n1"));
    when(rawStore.unwrap(TableStore.class)).thenReturn(tableStore);
    when(tableStore.getPartitionsByNames(eq(TABLE), any(GetPartitionsArgs.class)))
        .thenAnswer(inv -> {
          GetPartitionsArgs args = inv.getArgument(1);
          return args.getPartNames().stream().map(TestPartitionsIterable::mkPart).collect(Collectors.toList());
        });

    List<List<Partition>> batches = toBatches(PartitionsIterable.create(rawStore, TABLE, 1));

    Assert.assertEquals(2, batches.size());
    verify(rawStore).listPartitionNames("hive", "default", "t", (short) -1);
  }

  @Test
  public void skipColumnSchemaForPartitionForwarded() throws Exception {
    TableStore store = mock(TableStore.class);
    when(store.getPartitionsByNames(eq(TABLE), any(GetPartitionsArgs.class)))
        .thenReturn(Collections.singletonList(mkPart("p0")));

    PartitionsIterable iterable = PartitionsIterable.create(store, TABLE, 10,
        Collections.singletonList("p0")).withSkipColumnSchemaForPartition(true);
    toBatches(iterable);

    verify(store).getPartitionsByNames(eq(TABLE), org.mockito.ArgumentMatchers.argThat(
        args -> args.isSkipColumnSchemaForPartition() && args.getPartNames().equals(
            Collections.singletonList("p0"))));
  }
}
