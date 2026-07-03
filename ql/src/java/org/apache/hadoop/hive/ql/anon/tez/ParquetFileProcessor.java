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
import org.apache.hadoop.hive.ql.anon.anonymize.RowAnonymizer;
import org.apache.hadoop.hive.ql.anon.extract.Extractor;
import org.apache.hadoop.hive.ql.anon.utils.FileUtils;
import org.apache.hadoop.hive.ql.anon.utils.TezUtils;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.serde2.io.ParquetHiveRecord;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.RecordReader;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.hadoop.hive.ql.anon.consts.AnonConst.ANON_INDEX_VERIFY;
import static org.apache.hadoop.hive.ql.anon.utils.TezUtils.swapFiles;

public class ParquetFileProcessor implements FileProcessor {

  private final Configuration conf;
  private final Extractor extractor;
  private final RowAnonymizer rowAnonymizer;
  private final RowContext rowContext;
  private final Text textIdFieldName = new Text();
  private final Set<WritableComparable> keySet;
  private final int numKeys;
  private final Set<WritableComparable> idValueBuffer = new HashSet<>();
  private final boolean indexVerify;
  private final Stats stats = new Stats();

  public ParquetFileProcessor(final Configuration conf, final Extractor extractor, final RowAnonymizer rowAnonymizer, final RowContext rowContext, final List<WritableComparable> keys) {
    this.conf = conf;
    this.extractor = extractor;
    this.rowAnonymizer = rowAnonymizer;
    this.rowContext = rowContext;
    this.keySet = new HashSet<>(keys);
    this.numKeys = keys.size();
    this.indexVerify = conf == null ? false : conf.getBoolean(ANON_INDEX_VERIFY, false);
    textIdFieldName.set(rowContext.getIdFieldName());
  }

  @Override
  public void processFile(final AnonContext context) throws IOException {
    final RecordReader<NullWritable, ArrayWritable> reader = FileUtils.getReader(context.getInputPath(), conf);
    final long start = System.currentTimeMillis();
    try {
      final FileSinkOperator.RecordWriter writer = FileUtils.getWriter(context.getTmpPath(), conf);
      try {
        final NullWritable key = reader.createKey();
        final ArrayWritable value = reader.createValue();
        final ParquetHiveRecord record = new ParquetHiveRecord();
        record.inspector = FileUtils.getOI(conf);
        while (reader.next(key, value)) {
          record.value = processFileRow(value);
          writer.write(record);
        }
      } finally {
        writer.close(false);
      }
      stats.totalProcessingTime += System.currentTimeMillis() - start;
      stats.numKeys = numKeys;
      if (TezUtils.swapFiles(conf, context)) {
        stats.filesRewritten++;
      }
    } catch (IOException | RuntimeException e) {
      TezUtils.deleteQuietly(conf, context.getTmpPath());
      throw e;
    } finally {
      reader.close();
    }
  }

  private Object processFileRow(final Object rowIn) {
    stats.visitedMessages++;

    final Writable[] rowValues = ((ArrayWritable) rowIn).get();
    final Writable body = rowValues[rowContext.getBodyIx()];
    final WritableComparable msgId = (WritableComparable) rowValues[rowContext.getMsgIdIx()];

    if (!rowAnonymizer.matches(msgId)) {
      return rowIn;
    }
    if (!extractor.containsIdentityField(rowContext.getIdFieldName(), msgId, body)) {
      return rowIn;
    }
    stats.piiMessages++;

    if (!identityInKeySet(msgId, body)) {
      return rowIn;
    }

    final long start = System.currentTimeMillis();
    final Writable anonymized = rowAnonymizer.anonymize(msgId, body);
    rowValues[rowContext.getBodyIx()] = anonymized;
    final long end = System.currentTimeMillis();

    stats.totalAnonTime += (end - start);
    stats.anonymizedMessages++;
    return rowIn;
  }

  private boolean identityInKeySet(final WritableComparable msgId, final Writable body) {
    idValueBuffer.clear();
    extractor.extractIdentifyFieldValues(textIdFieldName, msgId, body, idValueBuffer);
    final Set<WritableComparable> probe = idValueBuffer.size() < keySet.size() ? idValueBuffer : keySet;
    final Set<WritableComparable> target = (probe == idValueBuffer) ? keySet : idValueBuffer;
    for (final WritableComparable v : probe) {
      if (target.contains(v)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public void processFile(final AnonContext context, final Writable value1) throws IOException {
    final MapWritable locatorToSchema = (MapWritable) value1;
    final RecordReader<NullWritable, ArrayWritable> reader = FileUtils.getReader(context.getInputPath(), conf);
    final long start = System.currentTimeMillis();
    try {
      final FileSinkOperator.RecordWriter writer = FileUtils.getWriter(context.getTmpPath(), conf);
      try {
        final NullWritable key = reader.createKey();
        final ArrayWritable value = reader.createValue();
        final ParquetHiveRecord record = new ParquetHiveRecord();
        record.inspector = FileUtils.getOI(conf);
        while (reader.next(key, value)) {
          record.value = processFileRow(value, locatorToSchema);
          writer.write(record);
        }
      } finally {
        writer.close(false);
      }
      stats.totalProcessingTime += System.currentTimeMillis() - start;
      if (swapFiles(conf, context)) {
        stats.filesRewritten++;
      }
    } catch (IOException | RuntimeException e) {
      TezUtils.deleteQuietly(conf, context.getTmpPath());
      throw e;
    } finally {
      reader.close();
    }
  }

  private Object processFileRow(final Object rowIn, final MapWritable locatorToSchema) {
    stats.visitedMessages++;

    final Writable[] rowValues = ((ArrayWritable) rowIn).get();
    final WritableComparable msgOffset = (WritableComparable) rowValues[rowContext.getMsgOffsetIx()];

    if (!locatorToSchema.containsKey(msgOffset)) {
      return rowIn;
    }
    stats.piiMessages++;

    final WritableComparable msgId = (WritableComparable) rowValues[rowContext.getMsgIdIx()];
    final Writable body = (Writable) rowValues[rowContext.getBodyIx()];
    if (indexVerify
        && !(extractor.containsIdentityField(rowContext.getIdFieldName(), msgId, body)
            && identityInKeySet(msgId, body))) {
      return rowIn;
    }
    long start = System.currentTimeMillis();

    final Writable anonymized = rowAnonymizer.anonymize(msgId, body);
    rowValues[rowContext.getBodyIx()] = anonymized;

    long end = System.currentTimeMillis();
    stats.totalAnonTime += (end - start);
    stats.anonymizedMessages++;
    return rowIn;
  }

  public Stats getStats() {
    return stats;
  }
}
