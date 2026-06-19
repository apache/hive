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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.ColumnInternalFormat;
import org.apache.hadoop.hive.ql.anon.ConstCode;
import org.apache.hadoop.hive.ql.anon.FileType;
import org.apache.hadoop.hive.ql.anon.anonymize.RowAnonymizer;
import org.apache.hadoop.hive.ql.anon.extract.Extractor;
import org.apache.hadoop.hive.ql.anon.extract.ExtractorFactory;
import org.apache.hadoop.hive.ql.anon.policy.DataErasurePolicy;
import org.apache.hadoop.hive.ql.anon.utils.Utils;
import org.apache.hadoop.io.MapWritable;
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

import java.util.List;
import java.util.Map;

import static org.apache.hadoop.hive.ql.anon.consts.AnonConst.*;
import static org.apache.hadoop.hive.ql.anon.utils.Utils.parsePidList;

public class AnonProcessor extends AbstractLogicalIOProcessor {

  private static final Logger LOG = LoggerFactory.getLogger(AnonProcessor.class);
  private Configuration conf;
  private RowAnonymizer rowAnonymizer;

  private boolean indexFound;
  private Extractor extractor;
  private List<WritableComparable> keys;
  private RowContext rowContext;
  private FileProcessor fileProcessor;

  public AnonProcessor(ProcessorContext context) {
    super(context);
  }

  @Override
  public void initialize() throws Exception {
    conf = TezUtils.createConfFromUserPayload(getContext().getUserPayload());
    final String policyDoc = conf.get(ANON_POLICY_DOC);
    final DataErasurePolicy erasurePolicy = DataErasurePolicy.fromString(policyDoc);
    rowAnonymizer = new RowAnonymizer(conf, erasurePolicy);

    final String pidCsv = conf.get(ANON_PID_LIST);
    keys = parsePidList(pidCsv, erasurePolicy.identityFieldType);

    indexFound = conf.getBoolean(ANON_INDEX_FOUND, false);
    final String identityFldName = erasurePolicy.identityFieldName.replace("'", "");
    final int msgOffsetIx = requireNonNegativeIx(ANON_MSG_OFFSET_IX);
    final int msgIdIx = requireNonNegativeIx(ANON_MSG_ID_IX);
    final int bodyIx = requireNonNegativeIx(ANON_BODY_IX);

    final ColumnInternalFormat internalFormat = ColumnInternalFormat.valueOf(conf.get(ANON_COLUMN_INTERNAL_FORMAT));
    final ConstCode code = Utils.getConstCode(internalFormat);
    extractor = ExtractorFactory.getExtractor(code);

    rowContext = new RowContext(msgIdIx, msgOffsetIx, bodyIx, identityFldName, internalFormat);

    final String ft = conf.get(ANON_FILE_TYPE);
    final FileType fileType = FileType.valueOf(ft);
    fileProcessor = FileProcessorFactory.create(fileType, conf, extractor, rowAnonymizer, rowContext, keys);

    final ProcessorContext ctx = getContext();
    LOG.info("[{}#{}] AnonProcessor ready (indexFound={}, format={}, fileType={}, keys={})",
        ctx.getTaskVertexName(), ctx.getTaskIndex(),
        indexFound, internalFormat, fileType, keys.size());
  }

  private int requireNonNegativeIx(final String key) {
    final int v = conf.getInt(key, -1);
    if (v < 0) {
      throw new IllegalStateException(
          "AnonProcessor config key '" + key + "' is missing or negative (got " + v + ")");
    }
    return v;
  }

  @Override
  public void handleEvents(List<Event> processorEvents) {
  }

  @Override
  public void close() throws Exception {
  }

  @Override
  public void run(Map<String, LogicalInput> inputs, Map<String, LogicalOutput> outputs) throws Exception {
    for (final LogicalInput input : inputs.values()) {
      input.start();
    }

    final String inputName = indexFound ? ANON_EDGE_INDEX : ANON_EDGE_TBL;
    final LogicalInput input = inputs.get(inputName);
    if (input == null) {
      throw new IllegalStateException("expected input edge '" + inputName
          + "' not present; available=" + inputs.keySet());
    }
    final Object reader = input.getReader();
    try {
      if (reader instanceof KeyValueReader) {
        consumeKeyValue((KeyValueReader) reader);
      } else if (reader instanceof KeyValuesReader) {
        consumeKeyValues((KeyValuesReader) reader);
      } else {
        throw new IllegalStateException("unexpected reader type: "
            + (reader == null ? "null" : reader.getClass().getName()));
      }
    } finally {
      emitFileCounters();
    }
  }

  private void emitFileCounters() {
    if (fileProcessor == null) {
      return;
    }
    final Stats s = fileProcessor.getStats();
    if (s == null || s.filesRewritten <= 0L) {
      return;
    }
    try {
      getContext().getCounters()
          .findCounter(ANON_COUNTER_GROUP, ANON_CTR_FILES_REWRITTEN)
          .increment(s.filesRewritten);
    } catch (Throwable t) {
      LOG.warn("AnonProcessor: could not emit {} counter: {}",
          ANON_CTR_FILES_REWRITTEN, t.getMessage());
    }
  }

  private void consumeKeyValue(final KeyValueReader kvReader) throws Exception {
    while (kvReader.next()) {
      final AnonContext context = new AnonContext(kvReader.getCurrentValue());
      fileProcessor.processFile(context);
    }
  }

  private void consumeKeyValues(final KeyValuesReader kvReader) throws Exception {
    while (kvReader.next()) {
      for (final Object value : kvReader.getCurrentValues()) {
        final MapWritable mw = Utils.convert(value);
        for (final Map.Entry<Writable, Writable> entry : mw.entrySet()) {
          fileProcessor.processFile(new AnonContext(entry.getKey()), entry.getValue());
        }
      }
    }
  }
}
