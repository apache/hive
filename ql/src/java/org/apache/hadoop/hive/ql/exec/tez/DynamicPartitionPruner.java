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

package org.apache.hadoop.hive.ql.exec.tez;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import org.apache.commons.lang3.mutable.MutableInt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluator;
import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluatorFactory;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.tez.dag.api.event.VertexState;
import org.apache.tez.runtime.api.InputInitializerContext;
import org.apache.tez.runtime.api.events.InputInitializerEvent;

/**
 * DynamicPartitionPruner takes a list of assigned partitions at runtime (split
 * generation) and prunes them using events generated during execution of the
 * dag.
 *
 */
public class DynamicPartitionPruner {

  private static final Logger LOG = LoggerFactory.getLogger(DynamicPartitionPruner.class);

  private final InputInitializerContext context;
  private final MapWork work;
  private final JobConf jobConf;


  private final Map<String, List<SourceInfo>> sourceInfoMap =
      new HashMap<String, List<SourceInfo>>();

  private final BytesWritable writable = new BytesWritable();

  /* Keeps track of all events that need to be processed - irrespective of the source */
  private final BlockingQueue<Object> queue = new LinkedBlockingQueue<Object>();

  /* Keeps track of vertices from which events are expected */
  private final Set<String> sourcesWaitingForEvents = new HashSet<String>();

  // Stores negative values to count columns. Eventually set to #tasks X #columns after the source vertex completes.
  private final Map<String, MutableInt> numExpectedEventsPerSource = new HashMap<>();
  private final Map<String, MutableInt> numEventsSeenPerSource = new HashMap<>();

  private int sourceInfoCount = 0;

  private final Object endOfEvents = new Object();

  private int totalEventCount = 0;

  public DynamicPartitionPruner(InputInitializerContext context, MapWork work, JobConf jobConf) throws
      SerDeException {
    this.context = context;
    this.work = work;
    this.jobConf = jobConf;
    synchronized (this) {
      initialize();
    }
  }

  public void prune()
      throws SerDeException, IOException,
      InterruptedException, HiveException {

    synchronized(sourcesWaitingForEvents) {

      if (sourcesWaitingForEvents.isEmpty()) {
        return;
      }

      Set<VertexState> states = Collections.singleton(VertexState.SUCCEEDED);
      for (String source : sourcesWaitingForEvents) {
        // we need to get state transition updates for the vertices that will send
        // events to us. once we have received all events and a vertex has succeeded,
        // we can move to do the pruning.
        context.registerForVertexStateUpdates(source, states);
      }
    }

    LOG.info("Waiting for events (" + sourceInfoCount + " sources) ...");
    // synchronous event processing loop. Won't return until all events have
    // been processed.
    this.processEvents();
    this.prunePartitions();
    LOG.info("Ok to proceed.");
  }

  public BlockingQueue<Object> getQueue() {
    return queue;
  }

  private void clear() {
    sourceInfoMap.clear();
    sourceInfoCount = 0;
  }

  private void initialize() throws SerDeException {
    this.clear();
    Map<String, SourceInfo> columnMap = new HashMap<String, SourceInfo>();
    // sources represent vertex names
    Set<String> sources = work.getEventSourceTableDescMap().keySet();

    sourcesWaitingForEvents.addAll(sources);

    for (String s : sources) {
      // Set to 0 to start with. This will be decremented for all columns for which events
      // are generated by this source - which is eventually used to determine number of expected
      // events for the source. #colums X #tasks
      numExpectedEventsPerSource.put(s, new MutableInt(0));
      numEventsSeenPerSource.put(s, new MutableInt(0));
      // Virtual relation generated by the reduce sync
      List<TableDesc> tables = work.getEventSourceTableDescMap().get(s);
      // Real column name - on which the operation is being performed
      List<String> columnNames = work.getEventSourceColumnNameMap().get(s);
      // Column type
      List<String> columnTypes = work.getEventSourceColumnTypeMap().get(s);
      // Expression for the operation. e.g. N^2 > 10
      List<ExprNodeDesc> partKeyExprs = work.getEventSourcePartKeyExprMap().get(s);
      // eventSourceTableDesc, eventSourceColumnName, evenSourcePartKeyExpr move in lock-step.
      // One entry is added to each at the same time

      Iterator<String> cit = columnNames.iterator();
      Iterator<String> typit = columnTypes.iterator();
      Iterator<ExprNodeDesc> pit = partKeyExprs.iterator();
      // A single source can process multiple columns, and will send an event for each of them.
      for (TableDesc t : tables) {
        numExpectedEventsPerSource.get(s).decrement();
        ++sourceInfoCount;
        String columnName = cit.next();
        String columnType = typit.next();
        ExprNodeDesc partKeyExpr = pit.next();
        SourceInfo si = createSourceInfo(t, partKeyExpr, columnName, columnType, jobConf);
        if (!sourceInfoMap.containsKey(s)) {
          sourceInfoMap.put(s, new ArrayList<SourceInfo>());
        }
        List<SourceInfo> sis = sourceInfoMap.get(s);
        sis.add(si);

        // We could have multiple sources restrict the same column, need to take
        // the union of the values in that case.
        if (columnMap.containsKey(columnName)) {
          // All Sources are initialized up front. Events from different sources will end up getting added to the same list.
          // Pruning is disabled if either source sends in an event which causes pruning to be skipped
          si.values = columnMap.get(columnName).values;
          si.skipPruning = columnMap.get(columnName).skipPruning;
        }
        columnMap.put(columnName, si);
      }
    }
  }

  private void prunePartitions() throws HiveException {
    int expectedEvents = 0;
    for (Map.Entry<String, List<SourceInfo>> entry : this.sourceInfoMap.entrySet()) {
      String source = entry.getKey();
      for (SourceInfo si : entry.getValue()) {
        int taskNum = context.getVertexNumTasks(source);
        LOG.info("Expecting " + taskNum + " events for vertex " + source + ", for column " + si.columnName);
        expectedEvents += taskNum;
        prunePartitionSingleSource(source, si);
      }
    }

    // sanity check. all tasks must submit events for us to succeed.
    if (expectedEvents != totalEventCount) {
      LOG.error("Expecting: " + expectedEvents + ", received: " + totalEventCount);
      throw new HiveException("Incorrect event count in dynamic partition pruning");
    }
  }

  @VisibleForTesting
  protected void prunePartitionSingleSource(String source, SourceInfo si)
      throws HiveException {

    if (si.skipPruning.get()) {
      // in this case we've determined that there's too much data
      // to prune dynamically.
      LOG.info("Skip pruning on " + source + ", column " + si.columnName);
      return;
    }

    Set<Object> values = si.values;
    String columnName = si.columnName;

    if (LOG.isDebugEnabled()) {
      StringBuilder sb = new StringBuilder("Pruning ");
      sb.append(columnName);
      sb.append(" with ");
      for (Object value : values) {
        sb.append(value == null ? null : value.toString());
        sb.append(", ");
      }
      LOG.debug(sb.toString());
    }

    ObjectInspector oi =
        PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(TypeInfoFactory
            .getPrimitiveTypeInfo(si.columnType));

    Converter converter =
        ObjectInspectorConverters.getConverter(
            PrimitiveObjectInspectorFactory.javaStringObjectInspector, oi);

    StructObjectInspector soi =
        ObjectInspectorFactory.getStandardStructObjectInspector(
            Collections.singletonList(columnName), Collections.singletonList(oi));

    @SuppressWarnings("rawtypes")
    ExprNodeEvaluator eval = ExprNodeEvaluatorFactory.get(si.partKey);
    eval.initialize(soi);

    applyFilterToPartitions(converter, eval, columnName, values);
  }

  @SuppressWarnings("rawtypes")
  private void applyFilterToPartitions(Converter converter, ExprNodeEvaluator eval,
      String columnName, Set<Object> values) throws HiveException {

    Object[] row = new Object[1];

    Iterator<Path> it = work.getPathToPartitionInfo().keySet().iterator();
    while (it.hasNext()) {
      Path p = it.next();
      PartitionDesc desc = work.getPathToPartitionInfo().get(p);
      Map<String, String> spec = desc.getPartSpec();
      if (spec == null) {
        throw new IllegalStateException("No partition spec found in dynamic pruning");
      }

      String partValueString = spec.get(columnName);
      if (partValueString == null) {
        throw new IllegalStateException("Could not find partition value for column: " + columnName);
      }

      Object partValue = converter.convert(partValueString);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Converted partition value: " + partValue + " original (" + partValueString + ")");
      }

      row[0] = partValue;
      partValue = eval.evaluate(row);
      if (LOG.isDebugEnabled()) {
        LOG.debug("part key expr applied: " + partValue);
      }

      if (!values.contains(partValue)) {
        LOG.info("Pruning path: " + p);
        it.remove();
        // work.removePathToPartitionInfo(p);
        work.removePathToAlias(p);
      }
    }
  }

  @VisibleForTesting
  protected SourceInfo createSourceInfo(TableDesc t, ExprNodeDesc partKeyExpr, String columnName,
                                        String columnType, JobConf jobConf) throws
      SerDeException {
    return new SourceInfo(t, partKeyExpr, columnName, columnType, jobConf);

  }

  @SuppressWarnings("deprecation")
  @VisibleForTesting
  static class SourceInfo {
    public final ExprNodeDesc partKey;
    public final Deserializer deserializer;
    public final StructObjectInspector soi;
    public final StructField field;
    public final ObjectInspector fieldInspector;
    /* List of partitions that are required - populated from processing each event */
    public Set<Object> values = new HashSet<Object>();
    /* Whether to skipPruning - depends on the payload from an event which may signal skip - if the event payload is too large */
    public AtomicBoolean skipPruning = new AtomicBoolean();
    public final String columnName;
    public final String columnType;

    @VisibleForTesting // Only used for testing.
    SourceInfo(TableDesc table, ExprNodeDesc partKey, String columnName, String columnType, JobConf jobConf, Object forTesting) {
      this.partKey = partKey;
      this.columnName = columnName;
      this.columnType = columnType;
      this.deserializer = null;
      this.soi = null;
      this.field = null;
      this.fieldInspector = null;
    }

    public SourceInfo(TableDesc table, ExprNodeDesc partKey, String columnName, String columnType, JobConf jobConf)
        throws SerDeException {

      this.skipPruning.set(false);

      this.partKey = partKey;

      this.columnName = columnName;
      this.columnType = columnType;

      deserializer = ReflectionUtils.newInstance(table.getDeserializerClass(), null);
      deserializer.initialize(jobConf, table.getProperties());

      ObjectInspector inspector = deserializer.getObjectInspector();
      LOG.debug("Type of obj insp: " + inspector.getTypeName());

      soi = (StructObjectInspector) inspector;
      List<? extends StructField> fields = soi.getAllStructFieldRefs();
      if (fields.size() > 1) {
        LOG.error("expecting single field in input");
      }

      field = fields.get(0);

      fieldInspector =
          ObjectInspectorUtils.getStandardObjectInspector(field.getFieldObjectInspector());
    }
  }

  private void processEvents() throws SerDeException, IOException, InterruptedException {
    int eventCount = 0;

    while (true) {
      Object element = queue.take();

      if (element == endOfEvents) {
        // we're done processing events
        break;
      }

      InputInitializerEvent event = (InputInitializerEvent) element;

      LOG.info("Input event: " + event.getTargetInputName() + ", " + event.getTargetVertexName()
          + ", " + (event.getUserPayload().limit() - event.getUserPayload().position()));
      processPayload(event.getUserPayload(), event.getSourceVertexName());
      eventCount += 1;
    }
    LOG.info("Received events: " + eventCount);
  }

  @SuppressWarnings("deprecation")
  @VisibleForTesting
  protected String processPayload(ByteBuffer payload, String sourceName) throws SerDeException,
      IOException {

    DataInputStream in = new DataInputStream(new ByteBufferBackedInputStream(payload));
    try {
      String columnName = in.readUTF();

      LOG.info("Source of event: " + sourceName);

      List<SourceInfo> infos = this.sourceInfoMap.get(sourceName);
      if (infos == null) {
        throw new IllegalStateException("no source info for event source: " + sourceName);
      }

      SourceInfo info = null;
      for (SourceInfo si : infos) {
        if (columnName.equals(si.columnName)) {
          info = si;
          break;
        }
      }

      if (info == null) {
        throw new IllegalStateException("no source info for column: " + columnName);
      }

      if (info.skipPruning.get()) {
        // Marked as skipped previously. Don't bother processing the rest of the payload.
      } else {
        boolean skip = in.readBoolean();
        if (skip) {
          info.skipPruning.set(true);
        } else {
          while (payload.hasRemaining()) {
            writable.readFields(in);

            Object row = info.deserializer.deserialize(writable);

            Object value = info.soi.getStructFieldData(row, info.field);
            value = ObjectInspectorUtils.copyToStandardObject(value, info.fieldInspector);

            if (LOG.isDebugEnabled()) {
              LOG.debug("Adding: " + value + " to list of required partitions");
            }
            info.values.add(value);
          }
        }
      }
    } finally {
      if (in != null) {
        in.close();
      }
    }
    return sourceName;
  }

  private static class ByteBufferBackedInputStream extends InputStream {

    ByteBuffer buf;

    public ByteBufferBackedInputStream(ByteBuffer buf) {
      this.buf = buf;
    }

    @Override
    public int read() throws IOException {
      if (!buf.hasRemaining()) {
        return -1;
      }
      return buf.get() & 0xFF;
    }

    @Override
    public int read(byte[] bytes, int off, int len) throws IOException {
      if (!buf.hasRemaining()) {
        return -1;
      }

      len = Math.min(len, buf.remaining());
      buf.get(bytes, off, len);
      return len;
    }
  }

  public void addEvent(InputInitializerEvent event) {
    synchronized(sourcesWaitingForEvents) {
      if (sourcesWaitingForEvents.contains(event.getSourceVertexName())) {
        ++totalEventCount;
        numEventsSeenPerSource.get(event.getSourceVertexName()).increment();
        if(!queue.offer(event)) {
          throw new IllegalStateException("Queue full");
        }
        checkForSourceCompletion(event.getSourceVertexName());
      }
    }
  }

  public void processVertex(String name) {
    LOG.info("Vertex succeeded: " + name);
    synchronized(sourcesWaitingForEvents) {
      // Get a deterministic count of number of tasks for the vertex.
      MutableInt prevVal = numExpectedEventsPerSource.get(name);
      int prevValInt = prevVal.intValue();
      Preconditions.checkState(prevValInt < 0,
          "Invalid value for numExpectedEvents for source: " + name + ", oldVal=" + prevValInt);
      prevVal.setValue((-1) * prevValInt * context.getVertexNumTasks(name));
      checkForSourceCompletion(name);
    }
  }

  private void checkForSourceCompletion(String name) {
    int expectedEvents = numExpectedEventsPerSource.get(name).getValue();
    if (expectedEvents < 0) {
      // Expected events not updated yet - vertex SUCCESS notification not received.
      return;
    } else {
      int processedEvents = numEventsSeenPerSource.get(name).getValue();
      if (processedEvents == expectedEvents) {
        sourcesWaitingForEvents.remove(name);
        if (sourcesWaitingForEvents.isEmpty()) {
          // we've got what we need; mark the queue
          if(!queue.offer(endOfEvents)) {
            throw new IllegalStateException("Queue full");
          }
        } else {
          LOG.info("Waiting for " + sourcesWaitingForEvents.size() + " sources.");
        }
      } else if (processedEvents > expectedEvents) {
        throw new IllegalStateException(
            "Received too many events for " + name + ", Expected=" + expectedEvents +
                ", Received=" + processedEvents);
      }
      return;
    }
  }
}
