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

package org.apache.hadoop.hive.ql.anon.tez;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.ColumnInternalFormat;
import org.apache.hadoop.hive.ql.anon.anonymize.RowProjector;
import org.apache.hadoop.hive.ql.anon.convert.BodyConverter;
import org.apache.hadoop.hive.ql.anon.convert.BodyConverterFactory;
import org.apache.hadoop.hive.ql.anon.model.BaseMsg;
import org.apache.hadoop.hive.ql.anon.policy.DataErasurePolicy;
import org.apache.hadoop.hive.ql.anon.utils.Utils;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.hadoop.hive.ql.io.orc.RecordReader;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.tez.common.TezUtils;
import org.apache.tez.runtime.api.AbstractLogicalIOProcessor;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.LogicalInput;
import org.apache.tez.runtime.api.LogicalOutput;
import org.apache.tez.runtime.api.ProcessorContext;
import org.apache.tez.runtime.library.api.KeyValueReader;
import org.apache.tez.runtime.library.api.KeyValuesReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.apache.hadoop.hive.ql.anon.consts.AnonConst.ANON_BODY_IX;
import static org.apache.hadoop.hive.ql.anon.consts.AnonConst.ANON_COLUMN_INTERNAL_FORMAT;
import static org.apache.hadoop.hive.ql.anon.consts.AnonConst.ANON_EDGE_INDEX;
import static org.apache.hadoop.hive.ql.anon.consts.AnonConst.ANON_EDGE_TBL;
import static org.apache.hadoop.hive.ql.anon.consts.AnonConst.ANON_INDEX_FOUND;
import static org.apache.hadoop.hive.ql.anon.consts.AnonConst.ANON_MSG_ID_IX;
import static org.apache.hadoop.hive.ql.anon.consts.AnonConst.ANON_MSG_OFFSET_IX;
import static org.apache.hadoop.hive.ql.anon.consts.AnonConst.ANON_PID_LIST;
import static org.apache.hadoop.hive.ql.anon.consts.AnonConst.ANON_POLICY_DOC;
public class ExtractProcessor extends AbstractLogicalIOProcessor {

  public static final String EXTRACT_STAGING_DIR = "anon.extract.staging.dir";

  private static final Logger LOG = LoggerFactory.getLogger(ExtractProcessor.class);
  private static final ObjectMapper JSON = new ObjectMapper();

  private Configuration conf;
  private RowProjector rowProjector;
  private BodyConverter converter;
  private int bodyIx;
  private int msgIdIx;
  private int msgOffsetIx;
  private boolean indexFound;
  private Path stagingDir;
  private BufferedWriter outputWriter;
  private String identityFieldName;
  private Set<String> requestedPids;

  public ExtractProcessor(final ProcessorContext context) {
    super(context);
  }

  @Override
  public void initialize() throws Exception {
    conf = TezUtils.createConfFromUserPayload(getContext().getUserPayload());
    final String policyDoc = conf.get(ANON_POLICY_DOC);
    final DataErasurePolicy policy = DataErasurePolicy.fromString(policyDoc);
    rowProjector = new RowProjector(conf, policy);
    identityFieldName = policy.identityFieldName == null
        ? null : policy.identityFieldName.replace("'", "");
    final String pidCsv = conf.get(ANON_PID_LIST, "");
    requestedPids = new HashSet<>();
    if (!pidCsv.isEmpty()) {
      for (final String pid : pidCsv.split(",")) {
        final String trimmed = pid.trim();
        if (!trimmed.isEmpty()) requestedPids.add(trimmed);
      }
    }

    indexFound = conf.getBoolean(ANON_INDEX_FOUND, false);
    bodyIx = requireNonNegativeIx(ANON_BODY_IX);
    msgIdIx = requireNonNegativeIx(ANON_MSG_ID_IX);
    msgOffsetIx = requireNonNegativeIx(ANON_MSG_OFFSET_IX);

    final ColumnInternalFormat colFormat =
        ColumnInternalFormat.valueOf(conf.get(ANON_COLUMN_INTERNAL_FORMAT));
    converter = BodyConverterFactory.getBodyConverter(colFormat);

    final String stagingDirStr = conf.get(EXTRACT_STAGING_DIR);
    if (stagingDirStr == null) {
      throw new IllegalStateException(EXTRACT_STAGING_DIR
          + " is not set; the analyzer must populate it before launching the DAG");
    }
    stagingDir = new Path(stagingDirStr);
    final FileSystem fs = stagingDir.getFileSystem(conf);
    fs.mkdirs(stagingDir);
    final ProcessorContext pc = getContext();
    final Path stagingFile = new Path(stagingDir,
        "part-" + String.format("%05d", pc.getTaskIndex()) + ".json");
    final FSDataOutputStream raw = fs.create(stagingFile, true);
    outputWriter = new BufferedWriter(
        new OutputStreamWriter(raw, StandardCharsets.UTF_8));

    LOG.info("[{}#{}] ExtractProcessor ready (indexFound={}, format={}, staging={})",
        pc.getTaskVertexName(), pc.getTaskIndex(),
        indexFound, colFormat, stagingFile);
  }

  private int requireNonNegativeIx(final String key) {
    final int v = conf.getInt(key, -1);
    if (v < 0) {
      throw new IllegalStateException(
          "ExtractProcessor config key '" + key + "' is missing or negative (got " + v + ")");
    }
    return v;
  }

  @Override
  public void handleEvents(final List<Event> processorEvents) {
  }

  @Override
  public void close() throws Exception {
    if (outputWriter != null) {
      outputWriter.close();
      outputWriter = null;
    }
  }

  @Override
  public void run(final Map<String, LogicalInput> inputs,
                  final Map<String, LogicalOutput> outputs) throws Exception {
    for (final LogicalInput input : inputs.values()) {
      input.start();
    }

    final String inputName = indexFound ? ANON_EDGE_INDEX : ANON_EDGE_TBL;
    final LogicalInput input = inputs.get(inputName);
    if (input == null) {
      throw new IllegalStateException("expected input edge '" + inputName
          + "' not present; available=" + inputs.keySet());
    }
    try {
      final Object reader = input.getReader();
      if (reader instanceof KeyValueReader) {
        consumeKeyValue((KeyValueReader) reader);
      } else if (reader instanceof KeyValuesReader) {
        consumeKeyValues((KeyValuesReader) reader);
      } else {
        throw new IllegalStateException("unexpected reader type: "
            + (reader == null ? "null" : reader.getClass().getName()));
      }
    } finally {
      try {
        close();
      } catch (Exception e) {
        LOG.warn("ExtractProcessor: failed to close staging writer after run: {}", e.getMessage());
      }
    }
  }

  private void consumeKeyValue(final KeyValueReader kvReader) throws Exception {
    while (kvReader.next()) {
      projectFile(new AnonContext(kvReader.getCurrentValue()),  null);
    }
  }

  private void consumeKeyValues(final KeyValuesReader kvReader) throws Exception {
    while (kvReader.next()) {
      for (final Object value : kvReader.getCurrentValues()) {
        final MapWritable mw =
            Utils.convert(value);
        for (final Map.Entry<Writable, Writable> entry : mw.entrySet()) {
          projectFile(new AnonContext(entry.getKey()), (MapWritable) entry.getValue());
        }
      }
    }
  }

  private void projectFile(final AnonContext context, final MapWritable locator) throws Exception {
    final Reader reader = OrcFile.createReader(context.getInputPath(),
        OrcFile.readerOptions(conf));
    final StructObjectInspector inputSOI = (StructObjectInspector) reader.getObjectInspector();
    final RecordReader recordReader = reader.rows();
    long matched = 0L;
    long scanned = 0L;
    try {
      Object rowIn = null;
      while (recordReader.hasNext()) {
        rowIn = recordReader.next(rowIn);
        scanned++;
        final List<Object> rowValues = inputSOI.getStructFieldsDataAsList(rowIn);
        final WritableComparable msgId =
            (WritableComparable) rowValues.get(msgIdIx);
        if (locator != null) {
          final WritableComparable msgOffset =
              (WritableComparable) rowValues.get(msgOffsetIx);
          if (!locator.containsKey(msgOffset)) {
            continue;
          }
        } else if (!rowProjector.matches(msgId)) {
          continue;
        }
        final Writable body = (Writable) rowValues.get(bodyIx);
        final Object msg = converter.convertBody(msgId, body);
        if (locator == null && !requestedPids.isEmpty()) {
          if (!matchesRequestedIdentity(msg)) {
            continue;
          }
        }
        final Map<String, Object> projection = rowProjector.project(msgId, msg);
        if (projection.isEmpty()) {
          continue;
        }
        emit(projection, msgId);
        matched++;
      }
    } finally {
      recordReader.close();
    }
    LOG.info("[{}] projected {}/{} rows from {}",
        getContext().getTaskVertexName(), matched, scanned, context.getInputPath());
  }

  private boolean matchesRequestedIdentity(final Object msg) {
    if (identityFieldName == null || identityFieldName.isEmpty()) {
      return true;
    }
    final Set<String> idStrings = collectIdentities(msg);
    if (idStrings.isEmpty()) {
      return false;
    }
    for (final String id : idStrings) {
      if (requestedPids.contains(id)) {
        return true;
      }
    }
    return false;
  }

  private Set<String> collectIdentities(final Object msg) {
    if (msg instanceof BaseMsg) {
      final Set<String> out = new HashSet<>();
      collectFromBaseMsg((BaseMsg) msg, out);
      return out;
    }
    return Collections.emptySet();
  }

  private void collectFromBaseMsg(final BaseMsg msg, final Set<String> out) {
    if (msg == null) return;
    final Field[] fields = msg.getClass().getDeclaredFields();
    for (final Field f : fields) {
      try {
        f.setAccessible(true);
        if (f.getName().equals(identityFieldName)) {
          final Object v = f.get(msg);
          if (v != null) out.add(String.valueOf(v));
          continue;
        }
        final Class<?> ft = f.getType();
        if (BaseMsg.class.isAssignableFrom(ft)) {
          collectFromBaseMsg((BaseMsg) f.get(msg), out);
        } else if (List.class.isAssignableFrom(ft)) {
          final Object lv = f.get(msg);
          if (lv instanceof List) {
            for (final Object e : (List<?>) lv) {
              if (e instanceof BaseMsg) collectFromBaseMsg((BaseMsg) e, out);
            }
          }
        }
      } catch (IllegalAccessException ignored) {
      }
    }
  }

  private void emit(final Map<String, Object> projection,
                    final WritableComparable msgId) throws IOException {
    final Map<String, Object> envelope = new LinkedHashMap<>();
    envelope.put("__schema", msgId == null ? null : msgId.toString());
    envelope.putAll(projection);
    outputWriter.write(JSON.writeValueAsString(envelope));
    outputWriter.newLine();
  }
}
