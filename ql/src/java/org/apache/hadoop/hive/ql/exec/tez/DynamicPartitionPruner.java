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
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import javolution.testing.AssertionException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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

  private static final Log LOG = LogFactory.getLog(DynamicPartitionPruner.class);

  private final Map<String, List<SourceInfo>> sourceInfoMap =
      new HashMap<String, List<SourceInfo>>();

  private final BytesWritable writable = new BytesWritable();

  private final BlockingQueue<Object> queue = new LinkedBlockingQueue<Object>();

  private final Set<String> sourcesWaitingForEvents = new HashSet<String>();

  private int sourceInfoCount = 0;

  private final Object endOfEvents = new Object();

  private int totalEventCount = 0;

  public DynamicPartitionPruner() {
  }

  public void prune(MapWork work, JobConf jobConf, InputInitializerContext context)
      throws SerDeException, IOException,
      InterruptedException, HiveException {

    synchronized(sourcesWaitingForEvents) {
      initialize(work, jobConf);

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

    LOG.info("Waiting for events (" + sourceInfoCount + " items) ...");
    // synchronous event processing loop. Won't return until all events have
    // been processed.
    this.processEvents();
    this.prunePartitions(work, context);
    LOG.info("Ok to proceed.");
  }

  public BlockingQueue<Object> getQueue() {
    return queue;
  }

  private void clear() {
    sourceInfoMap.clear();
    sourceInfoCount = 0;
  }

  public void initialize(MapWork work, JobConf jobConf) throws SerDeException {
    this.clear();
    Map<String, SourceInfo> columnMap = new HashMap<String, SourceInfo>();
    Set<String> sources = work.getEventSourceTableDescMap().keySet();

    sourcesWaitingForEvents.addAll(sources);

    for (String s : sources) {
      List<TableDesc> tables = work.getEventSourceTableDescMap().get(s);
      List<String> columnNames = work.getEventSourceColumnNameMap().get(s);
      List<ExprNodeDesc> partKeyExprs = work.getEventSourcePartKeyExprMap().get(s);

      Iterator<String> cit = columnNames.iterator();
      Iterator<ExprNodeDesc> pit = partKeyExprs.iterator();
      for (TableDesc t : tables) {
        ++sourceInfoCount;
        String columnName = cit.next();
        ExprNodeDesc partKeyExpr = pit.next();
        SourceInfo si = new SourceInfo(t, partKeyExpr, columnName, jobConf);
        if (!sourceInfoMap.containsKey(s)) {
          sourceInfoMap.put(s, new ArrayList<SourceInfo>());
        }
        List<SourceInfo> sis = sourceInfoMap.get(s);
        sis.add(si);

        // We could have multiple sources restrict the same column, need to take
        // the union of the values in that case.
        if (columnMap.containsKey(columnName)) {
          si.values = columnMap.get(columnName).values;
          si.skipPruning = columnMap.get(columnName).skipPruning;
        }
        columnMap.put(columnName, si);
      }
    }
  }

  private void prunePartitions(MapWork work, InputInitializerContext context) throws HiveException {
    int expectedEvents = 0;
    for (String source : this.sourceInfoMap.keySet()) {
      for (SourceInfo si : this.sourceInfoMap.get(source)) {
        int taskNum = context.getVertexNumTasks(source);
        LOG.info("Expecting " + taskNum + " events for vertex " + source);
        expectedEvents += taskNum;
        prunePartitionSingleSource(source, si, work);
      }
    }

    // sanity check. all tasks must submit events for us to succeed.
    if (expectedEvents != totalEventCount) {
      LOG.error("Expecting: " + expectedEvents + ", received: " + totalEventCount);
      throw new HiveException("Incorrect event count in dynamic parition pruning");
    }
  }

  private void prunePartitionSingleSource(String source, SourceInfo si, MapWork work)
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
            .getPrimitiveTypeInfo(si.fieldInspector.getTypeName()));

    Converter converter =
        ObjectInspectorConverters.getConverter(
            PrimitiveObjectInspectorFactory.javaStringObjectInspector, oi);

    StructObjectInspector soi =
        ObjectInspectorFactory.getStandardStructObjectInspector(
            Collections.singletonList(columnName), Collections.singletonList(oi));

    @SuppressWarnings("rawtypes")
    ExprNodeEvaluator eval = ExprNodeEvaluatorFactory.get(si.partKey);
    eval.initialize(soi);

    applyFilterToPartitions(work, converter, eval, columnName, values);
  }

  @SuppressWarnings("rawtypes")
  private void applyFilterToPartitions(MapWork work, Converter converter, ExprNodeEvaluator eval,
      String columnName, Set<Object> values) throws HiveException {

    Object[] row = new Object[1];

    Iterator<String> it = work.getPathToPartitionInfo().keySet().iterator();
    while (it.hasNext()) {
      String p = it.next();
      PartitionDesc desc = work.getPathToPartitionInfo().get(p);
      Map<String, String> spec = desc.getPartSpec();
      if (spec == null) {
        throw new AssertionException("No partition spec found in dynamic pruning");
      }

      String partValueString = spec.get(columnName);
      if (partValueString == null) {
        throw new AssertionException("Could not find partition value for column: " + columnName);
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
        work.getPathToAliases().remove(p);
        work.getPaths().remove(p);
        work.getPartitionDescs().remove(desc);
      }
    }
  }

  @SuppressWarnings("deprecation")
  private static class SourceInfo {
    public final ExprNodeDesc partKey;
    public final Deserializer deserializer;
    public final StructObjectInspector soi;
    public final StructField field;
    public final ObjectInspector fieldInspector;
    public Set<Object> values = new HashSet<Object>();
    public AtomicBoolean skipPruning = new AtomicBoolean();
    public final String columnName;

    public SourceInfo(TableDesc table, ExprNodeDesc partKey, String columnName, JobConf jobConf)
        throws SerDeException {

      this.skipPruning.set(false);

      this.partKey = partKey;

      this.columnName = columnName;

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
  private String processPayload(ByteBuffer payload, String sourceName) throws SerDeException,
      IOException {

    DataInputStream in = new DataInputStream(new ByteBufferBackedInputStream(payload));
    String columnName = in.readUTF();
    boolean skip = in.readBoolean();

    LOG.info("Source of event: " + sourceName);

    List<SourceInfo> infos = this.sourceInfoMap.get(sourceName);
    if (infos == null) {
      in.close();
      throw new AssertionException("no source info for event source: " + sourceName);
    }

    SourceInfo info = null;
    for (SourceInfo si : infos) {
      if (columnName.equals(si.columnName)) {
        info = si;
        break;
      }
    }

    if (info == null) {
      in.close();
      throw new AssertionException("no source info for column: " + columnName);
    }

    if (skip) {
      info.skipPruning.set(true);
    }

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
    in.close();
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
        queue.offer(event);
      }
    }
  }

  public void processVertex(String name) {
    LOG.info("Vertex succeeded: " + name);

    synchronized(sourcesWaitingForEvents) {
      sourcesWaitingForEvents.remove(name);

      if (sourcesWaitingForEvents.isEmpty()) {
        // we've got what we need; mark the queue
        queue.offer(endOfEvents);
      } else {
        LOG.info("Waiting for " + sourcesWaitingForEvents.size() + " events.");
      }
    }
  }
}
