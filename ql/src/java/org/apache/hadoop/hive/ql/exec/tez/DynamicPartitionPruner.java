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

package org.apache.hadoop.hive.ql.exec.tez;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.hadoop.hive.ql.exec.SerializationUtilities;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDynamicListDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPAnd;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluator;
import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluatorFactory;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
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

  private InputInitializerContext context;
  private MapWork work;
  private JobConf jobConf;

  private final Map<String, List<SourceInfo>> sourceInfoMap = new HashMap<>();

  private final BytesWritable writable = new BytesWritable();

  /* Keeps track of all events that need to be processed - irrespective of the source */
  private final BlockingQueue<Object> queue = new LinkedBlockingQueue<>();
  private final BlockingQueue<String> finishedVertices = new LinkedBlockingQueue<>();
  private static final Object VERTEX_FINISH_TOKEN = new Object();

  /* Keeps track of vertices from which events are expected */
  private final Set<String> sourcesWaitingForEvents = new HashSet<String>();

  // Stores negative values to count columns. Eventually set to #tasks X #columns after the source vertex completes.
  private final Map<String, MutableInt> numExpectedEventsPerSource = new HashMap<>();
  private final Map<String, MutableInt> numEventsSeenPerSource = new HashMap<>();

  private int sourceInfoCount = 0;

  private int totalEventCount = 0;

  public void prune() throws SerDeException, IOException, InterruptedException, HiveException {
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

    LOG.info("Waiting for events ({} sources) ...", sourceInfoCount);
    // synchronous event processing loop. Won't return until all events have
    // been processed.
    this.processEvents();
    this.prunePartitions();
    LOG.info("Ok to proceed.");
  }

  private void clear() {
    sourceInfoMap.clear();
    sourceInfoCount = 0;
  }

  public void initialize(InputInitializerContext context, MapWork work, JobConf jobConf) throws SerDeException {
    this.clear();
    this.context = context;
    this.work = work;
    this.jobConf = jobConf;

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
      // Expression for pruning
      List<ExprNodeDesc> predicates = work.getEventSourcePredicateExprMap().get(s);

      // eventSourceTableDesc, eventSourceColumnName, evenSourcePartKeyExpr move in lock-step.
      // One entry is added to each at the same time

      Iterator<String> cit = columnNames.iterator();
      Iterator<String> typit = columnTypes.iterator();
      Iterator<ExprNodeDesc> pit = partKeyExprs.iterator();
      Iterator<ExprNodeDesc> predit = predicates.iterator();
      // A single source can process multiple columns, and will send an event for each of them.
      for (TableDesc t : tables) {
        numExpectedEventsPerSource.get(s).decrement();
        ++sourceInfoCount;
        String columnName = cit.next();
        String columnType = typit.next();
        ExprNodeDesc partKeyExpr = pit.next();
        ExprNodeDesc predicate = predit.next();
        SourceInfo si = createSourceInfo(t, partKeyExpr, predicate, columnName, columnType, jobConf);
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
    List<ExprNodeDesc> prunerExprs = new LinkedList<>();
    for (Map.Entry<String, List<SourceInfo>> entry : this.sourceInfoMap.entrySet()) {
      String source = entry.getKey();
      for (SourceInfo si : entry.getValue()) {
        int taskNum = context.getVertexNumTasks(source);
        LOG.info("Expecting {} events for vertex {}, for column {}", taskNum, source, si.columnName);
        expectedEvents += taskNum;
        ExprNodeDesc prunerExpr = prunePartitionSingleSource(jobConf, source, si);
        if (prunerExpr != null) {
          prunerExprs.add(prunerExpr);
        }
      }
    }

    // If we have partition pruner expressions to push down then create the appropriate predicate
    if (prunerExprs.size() != 0) {
      ExprNodeGenericFuncDesc prunerExpr;
      if (prunerExprs.size() == 1) {
        prunerExpr = (ExprNodeGenericFuncDesc)prunerExprs.iterator().next();
      } else {
        // If we have multiple pruner expressions wrap them into an AND
        prunerExpr = new ExprNodeGenericFuncDesc(TypeInfoFactory.booleanTypeInfo, new GenericUDFOPAnd(), "and", prunerExprs);
      }

      // Push the predicate to the config for further use in HiveInputFormat
      jobConf.set(TableScanDesc.PARTITION_PRUNING_FILTER, SerializationUtilities.serializeExpression(prunerExpr));
    }

    // sanity check. all tasks must submit events for us to succeed.
    if (expectedEvents != totalEventCount) {
      LOG.error("Expecting: {} events, received: {}", expectedEvents, totalEventCount);
      throw new HiveException("Incorrect event count in dynamic partition pruning");
    }
  }

  @VisibleForTesting
  protected ExprNodeDesc prunePartitionSingleSource(JobConf jobConf, String source, SourceInfo si)
      throws HiveException {

    if (si.skipPruning.get()) {
      // in this case we've determined that there's too much data
      // to prune dynamically.
      LOG.info("Skip pruning on {}, column {}", source, si.columnName);
      return null;
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

    PrimitiveObjectInspector oi =
        PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(TypeInfoFactory
            .getPrimitiveTypeInfo(si.columnType));

    if (si.predicate != null) {
      // If we have a predicate defined then use that for pruning in the TableScan filter
      // Works for StorageHandlers where addDynamicSplitPruningEdge is defined
      List<ExprNodeConstantDesc> dynArgs = values.stream()
          .map(v -> new ExprNodeConstantDesc(oi.getPrimitiveJavaObject(v)))
          .collect(Collectors.toList());

      ExprNodeDesc clone = si.predicate.clone();

      replaceDynamicLists(clone, dynArgs);
      return clone;
    } else {
      // If we have to prune the path list for split generation
      Converter converter =
          ObjectInspectorConverters.getConverter(
              PrimitiveObjectInspectorFactory.javaStringObjectInspector, oi);

      StructObjectInspector soi =
          ObjectInspectorFactory.getStandardStructObjectInspector(
              Collections.singletonList(columnName), Collections.singletonList(oi));

      @SuppressWarnings("rawtypes")
      ExprNodeEvaluator eval = ExprNodeEvaluatorFactory.get(si.partKey);
      eval.initialize(soi);

      applyFilterToPartitions(converter, eval, columnName, values, si.mustKeepOnePartition);
      return null;
    }
  }

  @SuppressWarnings("rawtypes")
  private void applyFilterToPartitions(Converter converter, ExprNodeEvaluator eval,
      String columnName, Set<Object> values, boolean mustKeepOnePartition) throws HiveException {

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
      LOG.debug("Converted partition value: {} original ({})", partValue, partValueString);

      row[0] = partValue;
      partValue = eval.evaluate(row);
      LOG.debug("part key expr applied: {}", partValue);

      if (!values.contains(partValue) && (!mustKeepOnePartition || work.getPathToPartitionInfo().size() > 1)) {
        LOG.info("Pruning path: {}", p);
        it.remove();
        // work.removePathToPartitionInfo(p);
        work.removePathToAlias(p);
      }
    }
  }

  @VisibleForTesting
  protected SourceInfo createSourceInfo(TableDesc t, ExprNodeDesc partKeyExpr, ExprNodeDesc predicate, String columnName,
                                        String columnType, JobConf jobConf) throws
      SerDeException {
    return new SourceInfo(t, partKeyExpr, predicate, columnName, columnType, jobConf);

  }

  @SuppressWarnings("deprecation")
  @VisibleForTesting
  static class SourceInfo {
    public final ExprNodeDesc partKey;
    public final ExprNodeDesc predicate;
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
    private boolean mustKeepOnePartition;

    @VisibleForTesting // Only used for testing.
    SourceInfo(TableDesc table, ExprNodeDesc partKey, ExprNodeDesc predicate, String columnName, String columnType, JobConf jobConf, Object forTesting) {
      this.partKey = partKey;
      this.predicate = predicate;
      this.columnName = columnName;
      this.columnType = columnType;
      this.deserializer = null;
      this.soi = null;
      this.field = null;
      this.fieldInspector = null;
    }

    public SourceInfo(TableDesc table, ExprNodeDesc partKey, ExprNodeDesc predicate, String columnName, String columnType, JobConf jobConf)
        throws SerDeException {

      this.skipPruning.set(false);

      this.partKey = partKey;
      this.predicate = predicate;

      this.columnName = columnName;
      this.columnType = columnType;
      this.mustKeepOnePartition = jobConf.getBoolean(Utilities.ENSURE_OPERATORS_EXECUTED, false);

      AbstractSerDe serDe = ReflectionUtils.newInstance(table.getSerDeClass(), null);
      serDe.initialize(jobConf, table.getProperties(), null);

      this.deserializer = serDe;

      ObjectInspector inspector = deserializer.getObjectInspector();
      LOG.debug("Type of obj insp: {}", inspector.getTypeName());

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

      if (element == VERTEX_FINISH_TOKEN) {
        String updatedSource = finishedVertices.poll();
        calculateFinishCondition(updatedSource);
        if (checkForSourceCompletion(updatedSource)) {
          break;
        }
      } else {
        InputInitializerEvent event = (InputInitializerEvent) element;
        ByteBuffer payload = event.getUserPayload();
        numEventsSeenPerSource.computeIfAbsent(event.getSourceVertexName(), vn -> new MutableInt(0))
            .increment();

        totalEventCount++;
        LOG.info("Input event ({} -> {} {}), event payload size: {}", event.getSourceVertexName(),
            event.getTargetVertexName(), event.getTargetInputName(), (payload.limit() - payload.position()));
        processPayload(event.getUserPayload(), event.getSourceVertexName());
        eventCount += 1;
        if (checkForSourceCompletion(event.getSourceVertexName())) {
          break;
        }
      }
    }
    LOG.info("Received events: {}", eventCount);
  }

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
          int partitionCount = 0;
          while (payload.hasRemaining()) {
            writable.readFields(in);

            Object row = info.deserializer.deserialize(writable);

            Object value = info.soi.getStructFieldData(row, info.field);
            value = ObjectInspectorUtils.copyToStandardObject(value, info.fieldInspector);

            LOG.debug("Adding: {} to list of required partitions", value);

            info.values.add(value);
            partitionCount++;
          }
          LOG.info("Received {} partitions (source: {}, column: {})", partitionCount, sourceName, columnName);
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
    if(!queue.offer(event)) {
      throw new IllegalStateException("Queue full");
    }
  }

  private void calculateFinishCondition(String sourceName) {
    // Get a deterministic count of number of tasks for the vertex.
    MutableInt prevVal = numExpectedEventsPerSource.get(sourceName);
    int prevValInt = prevVal.intValue();
    Preconditions.checkState(prevValInt < 0,
        "Invalid value for numExpectedEvents for source: " + sourceName + ", oldVal=" + prevValInt);
    prevVal.setValue((-1) * prevValInt * context.getVertexNumTasks(sourceName));
  }

  public void processVertex(String name) {
    LOG.info("Vertex succeeded: {}", name);
    finishedVertices.add(name);
    queue.offer(VERTEX_FINISH_TOKEN);
  }

  private boolean checkForSourceCompletion(String name) {
    int expectedEvents = numExpectedEventsPerSource.get(name).getValue();
    if (expectedEvents < 0) {
      // Expected events not updated yet - vertex SUCCESS notification not received.
      return false;
    }

    int processedEvents = numEventsSeenPerSource.get(name).getValue();
    if (processedEvents == expectedEvents) {
      sourcesWaitingForEvents.remove(name);
      if (sourcesWaitingForEvents.isEmpty()) {
        return true;
      } else {
        LOG.info("Waiting for {} sources.", sourcesWaitingForEvents.size());
        return false;
      }
    } else if (processedEvents > expectedEvents) {
      throw new IllegalStateException(
          "Received too many events for " + name + ", Expected=" + expectedEvents +
              ", Received=" + processedEvents);
    }
    return false;
  }

  /**
   * Recursively replaces the ExprNodeDynamicListDesc to the list of the actual values. As a result of this call the
   * original expression is modified so it can be used for pushing down to the TableScan for filtering the data at the
   * source.
   * <p>
   * Please make sure to clone the predicate if needed since the original node will be modified.
   * @param node The node we are traversing
   * @param dynArgs The constant values we are substituting
   */
  private void replaceDynamicLists(ExprNodeDesc node, Collection<ExprNodeConstantDesc> dynArgs) {
    List<ExprNodeDesc> children = node.getChildren();
    if (children != null && !children.isEmpty()) {
      ListIterator<ExprNodeDesc> iterator = node.getChildren().listIterator();
      while (iterator.hasNext()) {
        ExprNodeDesc child = iterator.next();
        if (child instanceof ExprNodeDynamicListDesc) {
          iterator.remove();
          dynArgs.forEach(iterator::add);
        } else {
          replaceDynamicLists(child, dynArgs);
        }
      }
    }
  }
}
