/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.hive.ql.exec.tez;

import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.mapred.JobConf;
import org.apache.tez.runtime.api.InputInitializerContext;
import org.apache.tez.runtime.api.events.InputInitializerEvent;
import org.junit.Test;

public class TestDynamicPartitionPruner {

  @Test(timeout = 5000)
  public void testNoPruning() throws InterruptedException, IOException, HiveException,
      SerDeException {
    InputInitializerContext mockInitContext = mock(InputInitializerContext.class);
    MapWork mapWork = mock(MapWork.class);
    DynamicPartitionPruner pruner =
        new DynamicPartitionPrunerForEventTesting(mockInitContext, mapWork);

    PruneRunnable pruneRunnable = new PruneRunnable(pruner);
    Thread t = new Thread(pruneRunnable);
    t.start();
    try {
      pruneRunnable.start();
      pruneRunnable.awaitEnd();
      // Return immediately. No entries found for pruning. Verified via the timeout.
    } finally {
      t.interrupt();
      t.join();
    }
  }

  @Test(timeout = 5000)
  public void testSingleSourceOrdering1() throws InterruptedException, IOException, HiveException,
      SerDeException {
    InputInitializerContext mockInitContext = mock(InputInitializerContext.class);
    doReturn(1).when(mockInitContext).getVertexNumTasks("v1");

    MapWork mapWork = createMockMapWork(new TestSource("v1", 1));
    DynamicPartitionPruner pruner =
        new DynamicPartitionPrunerForEventTesting(mockInitContext, mapWork);


    PruneRunnable pruneRunnable = new PruneRunnable(pruner);
    Thread t = new Thread(pruneRunnable);
    t.start();
    try {
      pruneRunnable.start();

      InputInitializerEvent event =
          InputInitializerEvent.create("FakeTarget", "TargetInput", ByteBuffer.allocate(0));
      event.setSourceVertexName("v1");

      pruner.addEvent(event);
      pruner.processVertex("v1");

      pruneRunnable.awaitEnd();
      assertFalse(pruneRunnable.inError.get());
    } finally {
      t.interrupt();
      t.join();
    }
  }

  @Test(timeout = 5000)
  public void testSingleSourceOrdering2() throws InterruptedException, IOException, HiveException,
      SerDeException {
    InputInitializerContext mockInitContext = mock(InputInitializerContext.class);
    doReturn(1).when(mockInitContext).getVertexNumTasks("v1");

    MapWork mapWork = createMockMapWork(new TestSource("v1", 1));
    DynamicPartitionPruner pruner =
        new DynamicPartitionPrunerForEventTesting(mockInitContext, mapWork);


    PruneRunnable pruneRunnable = new PruneRunnable(pruner);
    Thread t = new Thread(pruneRunnable);
    t.start();
    try {
      pruneRunnable.start();

      InputInitializerEvent event =
          InputInitializerEvent.create("FakeTarget", "TargetInput", ByteBuffer.allocate(0));
      event.setSourceVertexName("v1");

      pruner.processVertex("v1");
      pruner.addEvent(event);

      pruneRunnable.awaitEnd();
      assertFalse(pruneRunnable.inError.get());
    } finally {
      t.interrupt();
      t.join();
    }
  }

  @Test(timeout = 5000)
  public void testSingleSourceMultipleFiltersOrdering1() throws InterruptedException, SerDeException {
    InputInitializerContext mockInitContext = mock(InputInitializerContext.class);
    doReturn(2).when(mockInitContext).getVertexNumTasks("v1");

    MapWork mapWork = createMockMapWork(new TestSource("v1", 2));
    DynamicPartitionPruner pruner =
        new DynamicPartitionPrunerForEventTesting(mockInitContext, mapWork);

    PruneRunnable pruneRunnable = new PruneRunnable(pruner);
    Thread t = new Thread(pruneRunnable);
    t.start();
    try {
      pruneRunnable.start();

      InputInitializerEvent event =
          InputInitializerEvent.create("FakeTarget", "TargetInput", ByteBuffer.allocate(0));
      event.setSourceVertexName("v1");

      pruner.addEvent(event);
      pruner.addEvent(event);
      pruner.addEvent(event);
      pruner.addEvent(event);
      pruner.processVertex("v1");

      pruneRunnable.awaitEnd();
      assertFalse(pruneRunnable.inError.get());
    } finally {
      t.interrupt();
      t.join();
    }
  }

  @Test(timeout = 5000)
  public void testSingleSourceMultipleFiltersOrdering2() throws InterruptedException, SerDeException {
    InputInitializerContext mockInitContext = mock(InputInitializerContext.class);
    doReturn(2).when(mockInitContext).getVertexNumTasks("v1");

    MapWork mapWork = createMockMapWork(new TestSource("v1", 2));
    DynamicPartitionPruner pruner =
        new DynamicPartitionPrunerForEventTesting(mockInitContext, mapWork);

    PruneRunnable pruneRunnable = new PruneRunnable(pruner);
    Thread t = new Thread(pruneRunnable);
    t.start();
    try {
      pruneRunnable.start();

      InputInitializerEvent event =
          InputInitializerEvent.create("FakeTarget", "TargetInput", ByteBuffer.allocate(0));
      event.setSourceVertexName("v1");

      pruner.processVertex("v1");
      pruner.addEvent(event);
      pruner.addEvent(event);
      pruner.addEvent(event);
      pruner.addEvent(event);

      pruneRunnable.awaitEnd();
      assertFalse(pruneRunnable.inError.get());
    } finally {
      t.interrupt();
      t.join();
    }
  }

  @Test(timeout = 5000)
  public void testMultipleSourcesOrdering1() throws InterruptedException, SerDeException {
    InputInitializerContext mockInitContext = mock(InputInitializerContext.class);
    doReturn(2).when(mockInitContext).getVertexNumTasks("v1");
    doReturn(3).when(mockInitContext).getVertexNumTasks("v2");

    MapWork mapWork = createMockMapWork(new TestSource("v1", 2), new TestSource("v2", 1));
    DynamicPartitionPruner pruner =
        new DynamicPartitionPrunerForEventTesting(mockInitContext, mapWork);

    PruneRunnable pruneRunnable = new PruneRunnable(pruner);
    Thread t = new Thread(pruneRunnable);
    t.start();
    try {
      pruneRunnable.start();

      InputInitializerEvent eventV1 =
          InputInitializerEvent.create("FakeTarget", "TargetInput", ByteBuffer.allocate(0));
      eventV1.setSourceVertexName("v1");

      InputInitializerEvent eventV2 =
          InputInitializerEvent.create("FakeTarget", "TargetInput", ByteBuffer.allocate(0));
      eventV2.setSourceVertexName("v2");

      // 2 X 2 events for V1. 3 X 1 events for V2

      pruner.addEvent(eventV1);
      pruner.addEvent(eventV1);
      pruner.addEvent(eventV1);
      pruner.addEvent(eventV1);
      pruner.addEvent(eventV2);
      pruner.addEvent(eventV2);
      pruner.addEvent(eventV2);
      pruner.processVertex("v1");
      pruner.processVertex("v2");

      pruneRunnable.awaitEnd();
      assertFalse(pruneRunnable.inError.get());
    } finally {
      t.interrupt();
      t.join();
    }
  }

  @Test(timeout = 5000)
  public void testMultipleSourcesOrdering2() throws InterruptedException, SerDeException {
    InputInitializerContext mockInitContext = mock(InputInitializerContext.class);
    doReturn(2).when(mockInitContext).getVertexNumTasks("v1");
    doReturn(3).when(mockInitContext).getVertexNumTasks("v2");

    MapWork mapWork = createMockMapWork(new TestSource("v1", 2), new TestSource("v2", 1));
    DynamicPartitionPruner pruner =
        new DynamicPartitionPrunerForEventTesting(mockInitContext, mapWork);

    PruneRunnable pruneRunnable = new PruneRunnable(pruner);
    Thread t = new Thread(pruneRunnable);
    t.start();
    try {
      pruneRunnable.start();

      InputInitializerEvent eventV1 =
          InputInitializerEvent.create("FakeTarget", "TargetInput", ByteBuffer.allocate(0));
      eventV1.setSourceVertexName("v1");

      InputInitializerEvent eventV2 =
          InputInitializerEvent.create("FakeTarget", "TargetInput", ByteBuffer.allocate(0));
      eventV2.setSourceVertexName("v2");

      // 2 X 2 events for V1. 3 X 1 events for V2

      pruner.processVertex("v1");
      pruner.processVertex("v2");
      pruner.addEvent(eventV1);
      pruner.addEvent(eventV1);
      pruner.addEvent(eventV1);
      pruner.addEvent(eventV1);
      pruner.addEvent(eventV2);
      pruner.addEvent(eventV2);
      pruner.addEvent(eventV2);

      pruneRunnable.awaitEnd();
      assertFalse(pruneRunnable.inError.get());
    } finally {
      t.interrupt();
      t.join();
    }
  }

  @Test(timeout = 5000)
  public void testMultipleSourcesOrdering3() throws InterruptedException, SerDeException {
    InputInitializerContext mockInitContext = mock(InputInitializerContext.class);
    doReturn(2).when(mockInitContext).getVertexNumTasks("v1");
    doReturn(3).when(mockInitContext).getVertexNumTasks("v2");

    MapWork mapWork = createMockMapWork(new TestSource("v1", 2), new TestSource("v2", 1));
    DynamicPartitionPruner pruner =
        new DynamicPartitionPrunerForEventTesting(mockInitContext, mapWork);

    PruneRunnable pruneRunnable = new PruneRunnable(pruner);
    Thread t = new Thread(pruneRunnable);
    t.start();
    try {
      pruneRunnable.start();

      InputInitializerEvent eventV1 =
          InputInitializerEvent.create("FakeTarget", "TargetInput", ByteBuffer.allocate(0));
      eventV1.setSourceVertexName("v1");

      InputInitializerEvent eventV2 =
          InputInitializerEvent.create("FakeTarget", "TargetInput", ByteBuffer.allocate(0));
      eventV2.setSourceVertexName("v2");

      // 2 X 2 events for V1. 3 X 1 events for V2
      pruner.addEvent(eventV1);
      pruner.addEvent(eventV1);
      pruner.processVertex("v1");
      pruner.addEvent(eventV1);
      pruner.addEvent(eventV1);
      pruner.addEvent(eventV2);
      pruner.processVertex("v2");
      pruner.addEvent(eventV2);
      pruner.addEvent(eventV2);

      pruneRunnable.awaitEnd();
      assertFalse(pruneRunnable.inError.get());
    } finally {
      t.interrupt();
      t.join();
    }
  }

  @Test(timeout = 5000, expected = IllegalStateException.class)
  public void testExtraEvents() throws InterruptedException, IOException, HiveException,
      SerDeException {
    InputInitializerContext mockInitContext = mock(InputInitializerContext.class);
    doReturn(1).when(mockInitContext).getVertexNumTasks("v1");

    MapWork mapWork = createMockMapWork(new TestSource("v1", 1));
    DynamicPartitionPruner pruner =
        new DynamicPartitionPrunerForEventTesting(mockInitContext, mapWork);


    PruneRunnable pruneRunnable = new PruneRunnable(pruner);
    Thread t = new Thread(pruneRunnable);
    t.start();
    try {
      pruneRunnable.start();

      InputInitializerEvent event =
          InputInitializerEvent.create("FakeTarget", "TargetInput", ByteBuffer.allocate(0));
      event.setSourceVertexName("v1");

      pruner.addEvent(event);
      pruner.addEvent(event);
      pruner.processVertex("v1");

      pruneRunnable.awaitEnd();
      assertFalse(pruneRunnable.inError.get());
    } finally {
      t.interrupt();
      t.join();
    }
  }

  @Test(timeout = 20000)
  public void testMissingEvent() throws InterruptedException, IOException, HiveException,
      SerDeException {
    InputInitializerContext mockInitContext = mock(InputInitializerContext.class);
    doReturn(1).when(mockInitContext).getVertexNumTasks("v1");

    MapWork mapWork = createMockMapWork(new TestSource("v1", 1));
    DynamicPartitionPruner pruner =
        new DynamicPartitionPrunerForEventTesting(mockInitContext, mapWork);


    PruneRunnable pruneRunnable = new PruneRunnable(pruner);
    Thread t = new Thread(pruneRunnable);
    t.start();
    try {
      pruneRunnable.start();

      InputInitializerEvent event =
          InputInitializerEvent.create("FakeTarget", "TargetInput", ByteBuffer.allocate(0));
      event.setSourceVertexName("v1");

      pruner.processVertex("v1");
      Thread.sleep(3000l);
      // The pruner should not have completed.
      assertFalse(pruneRunnable.ended.get());
      assertFalse(pruneRunnable.inError.get());
    } finally {
      t.interrupt();
      t.join();
    }
  }

  private static class PruneRunnable implements Runnable {

    final DynamicPartitionPruner pruner;
    final ReentrantLock lock = new ReentrantLock();
    final Condition endCondition = lock.newCondition();
    final Condition startCondition = lock.newCondition();
    final AtomicBoolean started = new AtomicBoolean(false);
    final AtomicBoolean ended = new AtomicBoolean(false);
    final AtomicBoolean inError = new AtomicBoolean(false);

    private PruneRunnable(DynamicPartitionPruner pruner) {
      this.pruner = pruner;
    }

    void start() {
      started.set(true);
      lock.lock();
      try {
        startCondition.signal();
      } finally {
        lock.unlock();
      }
    }

    void awaitEnd() throws InterruptedException {
      lock.lock();
      try {
        while (!ended.get()) {
          endCondition.await();
        }
      } finally {
        lock.unlock();
      }
    }

    @Override
    public void run() {
      try {
        lock.lock();
        try {
          while (!started.get()) {
            startCondition.await();
          }
        } finally {
          lock.unlock();
        }

        pruner.prune();
        lock.lock();
        try {
          ended.set(true);
          endCondition.signal();
        } finally {
          lock.unlock();
        }
      } catch (SerDeException | IOException | InterruptedException | HiveException e) {
        inError.set(true);
      }
    }
  }


  private MapWork createMockMapWork(TestSource... testSources) {
    MapWork mapWork = mock(MapWork.class);

    Map<String, List<TableDesc>> tableMap = new HashMap<>();
    Map<String, List<String>> columnMap = new HashMap<>();
    Map<String, List<String>> typeMap = new HashMap<>();
    Map<String, List<ExprNodeDesc>> exprMap = new HashMap<>();

    int count = 0;
    for (TestSource testSource : testSources) {

      for (int i = 0; i < testSource.numExpressions; i++) {
        List<TableDesc> tableDescList = tableMap.get(testSource.vertexName);
        if (tableDescList == null) {
          tableDescList = new LinkedList<>();
          tableMap.put(testSource.vertexName, tableDescList);
        }
        tableDescList.add(mock(TableDesc.class));

        List<String> columnList = columnMap.get(testSource.vertexName);
        if (columnList == null) {
          columnList = new LinkedList<>();
          columnMap.put(testSource.vertexName, columnList);
        }
        columnList.add(testSource.vertexName + "c_" + count + "_" + i);

	List<String> typeList = typeMap.get(testSource.vertexName);
        if (typeList == null) {
          typeList = new LinkedList<>();
          typeMap.put(testSource.vertexName, typeList);
        }
        typeList.add("string");

        List<ExprNodeDesc> exprNodeDescList = exprMap.get(testSource.vertexName);
        if (exprNodeDescList == null) {
          exprNodeDescList = new LinkedList<>();
          exprMap.put(testSource.vertexName, exprNodeDescList);
        }
        exprNodeDescList.add(mock(ExprNodeDesc.class));
      }

      count++;
    }

    doReturn(tableMap).when(mapWork).getEventSourceTableDescMap();
    doReturn(columnMap).when(mapWork).getEventSourceColumnNameMap();
    doReturn(exprMap).when(mapWork).getEventSourcePartKeyExprMap();
    doReturn(typeMap).when(mapWork).getEventSourceColumnTypeMap();
    return mapWork;
  }

  private static class TestSource {
    String vertexName;
    int numExpressions;

    public TestSource(String vertexName, int numExpressions) {
      this.vertexName = vertexName;
      this.numExpressions = numExpressions;
    }
  }

  private static class DynamicPartitionPrunerForEventTesting extends DynamicPartitionPruner {


    public DynamicPartitionPrunerForEventTesting(
        InputInitializerContext context, MapWork work) throws SerDeException {
      super(context, work, new JobConf());
    }

    @Override
    protected SourceInfo createSourceInfo(TableDesc t, ExprNodeDesc partKeyExpr, String columnName, String columnType,
                                          JobConf jobConf) throws
        SerDeException {
      return new SourceInfo(t, partKeyExpr, columnName, columnType, jobConf, null);
    }

    @Override
    protected String processPayload(ByteBuffer payload, String sourceName) throws SerDeException,
        IOException {
      // No-op: testing events only
      return sourceName;
    }

    @Override
    protected void prunePartitionSingleSource(String source, SourceInfo si)
        throws HiveException {
      // No-op: testing events only
    }
  }
}
