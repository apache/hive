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

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.crunch.types.orc.OrcUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.anon.anonymize.RowAnonymizer;
import org.apache.hadoop.hive.ql.anon.convert.BodyConverter;
import org.apache.hadoop.hive.ql.anon.convert.BodyConverterFactory;
import org.apache.hadoop.hive.ql.anon.extract.Extractor;
import org.apache.hadoop.hive.ql.anon.utils.FileUtils;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.hadoop.hive.ql.io.orc.RecordReader;
import org.apache.hadoop.hive.ql.io.orc.Writer;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import static org.apache.hadoop.hive.ql.anon.consts.AnonConst.ANON_INDEX_VERIFY;
import static org.apache.hadoop.hive.ql.anon.utils.TezUtils.deleteQuietly;
import static org.apache.hadoop.hive.ql.anon.utils.TezUtils.swapFiles;
public class OrcFileProcessor implements FileProcessor {

  private final Configuration conf;
  private final Extractor extractor;
  private final BodyConverter bodyConverter;
  private final RowAnonymizer rowAnonymizer;
  private final RowContext rowContext;
  private final Text textIdFieldName = new Text();
  private final Set<WritableComparable> keySet;
  private final boolean indexVerify;
  private TypeInfo typeInfo;
  private final Stats stats;

  private final Set<WritableComparable> idValueBuffer = new HashSet<>();
  private Object[] rowOutBuffer;

  public OrcFileProcessor(final Configuration conf, final Extractor extractor, final RowAnonymizer rowAnonymizer,
                          final RowContext rowContext, final List<WritableComparable> keys, final Stats stats) {
    this.conf = conf;
    this.extractor = extractor;
    this.rowAnonymizer = rowAnonymizer;
    this.rowContext = rowContext;
    this.keySet = new HashSet<>(keys);
    this.stats = stats;
    this.indexVerify = conf == null ? false : conf.getBoolean(ANON_INDEX_VERIFY, false);
    textIdFieldName.set(rowContext.getIdFieldName());
    bodyConverter = BodyConverterFactory.getBodyConverter(rowContext.getInternalFormat());
  }

  @Override
  public Stats getStats() {
    return stats;
  }

  @FunctionalInterface
  private interface RowGate {
    Object admit(Object rowIn, List<Object> rowValues);
  }


  @Override
  public void processFile(final AnonContext context) throws IOException {
    rewriteFile(context, this::admitNoIndex);
  }

  @Override
  public void processFile(final AnonContext context, final Writable value) throws IOException {
    final MapWritable locatorToSchema = (MapWritable) value;
    rewriteFile(context, (rowIn, values) -> admitByIndex(rowIn, values, locatorToSchema));
  }


  private void rewriteFile(final AnonContext context, final RowGate gate) throws IOException {
    final Reader reader = OrcFile.createReader(context.getInputPath(), OrcFile.readerOptions(conf));
    final StructObjectInspector inputSOI = (StructObjectInspector) reader.getObjectInspector();
    typeInfo = TypeInfoUtils.getTypeInfoFromObjectInspector(inputSOI);
    long anonNs = 0L;
    try {
      final Writer writer = FileUtils.getWriter(context.getTmpPath(), conf, inputSOI);
      try {
        final RecordReader recordReader = reader.rows();
        try {
          Object rowIn = null;
          while (recordReader.hasNext()) {
            rowIn = recordReader.next(rowIn);
            stats.visitedMessages++;
            final List<Object> rowValues = inputSOI.getStructFieldsDataAsList(rowIn);
            final Object msg = gate.admit(rowIn, rowValues);
            if (msg == null) {
              writer.addRow(rowIn);
              continue;
            }
            final long t0 = System.nanoTime();
            final Object rowOut = anonymiseRow(msg, rowValues);
            anonNs += System.nanoTime() - t0;
            stats.anonymizedMessages++;
            writer.addRow(rowOut);
          }
        } finally {
          recordReader.close();
        }
      } finally {
        writer.close();
      }
      stats.totalAnonTime += anonNs / 1_000_000L;
      if (swapFiles(conf, context)) {
        stats.filesRewritten++;
      }
    } catch (IOException | RuntimeException e) {
      deleteQuietly(conf, context.getTmpPath());
      throw e;
    }
  }


  private Object admitNoIndex(final Object rowIn, final List<Object> rowValues) {
    final Writable body = (Writable) rowValues.get(rowContext.getBodyIx());
    final WritableComparable msgId = (WritableComparable) rowValues.get(rowContext.getMsgIdIx());
    if (!rowAnonymizer.matches(msgId)) {
      return null;
    }
    if (!extractor.containsIdentityField(rowContext.getIdFieldName(), msgId, body)) {
      return null;
    }
    stats.piiMessages++;
    return matchByExtractor(msgId, body);
  }

  private Object admitByIndex(final Object rowIn, final List<Object> rowValues, final MapWritable locator) {
    final WritableComparable msgOffset = (WritableComparable) rowValues.get(rowContext.getMsgOffsetIx());
    if (!locator.containsKey(msgOffset)) {
      return null;
    }
    stats.piiMessages++;
    if (!indexVerify) {
      return INDEX_TRUSTED;
    }
    final Writable body = (Writable) rowValues.get(rowContext.getBodyIx());
    final WritableComparable msgId = (WritableComparable) rowValues.get(rowContext.getMsgIdIx());
    return matchByExtractor(msgId, body);
  }

  private Object matchByExtractor(final WritableComparable msgId, final Writable body) {
    final Object msg = bodyConverter.convertBody(msgId, body);
    idValueBuffer.clear();
    extractor.extract(textIdFieldName, msg, idValueBuffer);
    final Set<WritableComparable> probe;
    final Set<WritableComparable> target;
    if (idValueBuffer.size() < keySet.size()) {
      probe = idValueBuffer; target = keySet;
    } else {
      probe = keySet; target = idValueBuffer;
    }
    for (final WritableComparable v : probe) {
      if (target.contains(v)) {
        return msg;
      }
    }
    return null;
  }

  private static final Object INDEX_TRUSTED = new Object();


  private Object anonymiseRow(final Object msg, final List<Object> rowValues) {
    final WritableComparable msgId = (WritableComparable) rowValues.get(rowContext.getMsgIdIx());
    final Writable anonymised;
    if (msg == INDEX_TRUSTED) {
      final Writable body = (Writable) rowValues.get(rowContext.getBodyIx());
      anonymised = rowAnonymizer.anonymize(msgId, body);
    } else {
      final Object anonymisedMsg = rowAnonymizer.anonymize(msgId, msg);
      anonymised = bodyConverter.convertMessage(anonymisedMsg);
    }
    rowValues.set(rowContext.getBodyIx(), anonymised);
    final int n = rowValues.size();
    if (rowOutBuffer == null || rowOutBuffer.length != n) {
      rowOutBuffer = new Object[n];
    }
    for (int i = 0; i < n; i++) {
      rowOutBuffer[i] = rowValues.get(i);
    }
    return OrcUtils.createOrcStruct(typeInfo, rowOutBuffer);
  }
}
