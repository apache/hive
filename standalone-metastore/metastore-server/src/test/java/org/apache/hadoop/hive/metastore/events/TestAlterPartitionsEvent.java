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
package org.apache.hadoop.hive.metastore.events;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
@Category(MetastoreUnitTest.class)
public class TestAlterPartitionsEvent {

  private AlterPartitionsEvent event;
  private List<Partition> expectedParts;
  private Map<Long, List<Partition>> writeIdToParts;
  private int batchSize;
  private int expectedBatch;

  @Parameterized.Parameters
  public static Collection<Object[]> getIteratorToTest() {
    List<Partition> parts = new ArrayList<>();
    IntStream.range(0, 10).forEach(i -> {
          Partition partition = new Partition();
          partition.setValues(Arrays.asList(i + "", "part" + i));
          partition.setWriteId(i < 5 ? 1 : 2);
          parts.add(partition);
    });
    Partition partition = new Partition();
    partition.setValues(Arrays.asList(10 + "", "part" + 10));
    partition.setWriteId(3);
    parts.add(partition);

    AlterPartitionsEvent event = new AlterPartitionsEvent(parts, parts, new Table(), false, true, null);
    Collection<Object[]> params = new ArrayList<>();
    params.add(new Object[]{event, 1000, parts, 3});
    params.add(new Object[]{event, 5, parts, 3});
    params.add(new Object[]{event, 3, parts, 5});
    params.add(new Object[]{event, 2, parts, 7});
    params.add(new Object[]{event, 1, parts, 11});
    params.add(new Object[]{event, -1, parts, 3});
    return params;
  }

  public TestAlterPartitionsEvent(AlterPartitionsEvent event, int batchSize,
      List<Partition> expectedParts, int expectedBatch) {
    this.event = event;
    this.batchSize = batchSize;
    this.expectedParts = expectedParts;
    this.expectedBatch = expectedBatch;
    this.writeIdToParts = new HashMap<>();
    expectedParts.stream().forEach(partition ->
        writeIdToParts.computeIfAbsent(partition.getWriteId(), k -> new ArrayList<>()).add(partition));
  }

  @Test
  public void testGetNewPartitionsIterator() {
    int batch = 0;
    List<Partition> actual = new ArrayList<>();
    Map<Long, List<Partition>> idToParts = new HashMap<>();
    Iterator<List<Partition>> iterator = event.getNewPartsIterator(batchSize);
    while (iterator.hasNext()) {
      List<Partition> partitions = iterator.next();
      Assert.assertTrue(batchSize <=0 || partitions.size() <= batchSize);
      Long writeId = null;
      for (Partition part : partitions) {
        if (writeId == null) {
          writeId = part.getWriteId();
        } else {
          Assert.assertEquals(writeId.longValue(), part.getWriteId());
        }
      }
      idToParts.putIfAbsent(writeId, new ArrayList<>());
      idToParts.get(writeId).addAll(partitions);
      batch++;
      actual.addAll(partitions);
    }
    Assert.assertEquals(5, idToParts.get(1L).size());
    Assert.assertEquals(5, idToParts.get(2L).size());
    Assert.assertEquals(writeIdToParts, idToParts);
    Assert.assertEquals(expectedBatch, batch);
    Assert.assertEquals(expectedParts, actual);
  }

}
