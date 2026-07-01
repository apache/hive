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

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.IndexType;
import org.apache.hadoop.hive.ql.anon.btree.LocatorSchemaItem;
import org.apache.hadoop.hive.ql.anon.btree.StructValueList;
import org.apache.hadoop.hive.ql.anon.btree.ValueItem;
import org.apache.hadoop.hive.ql.anon.consts.AnonConst;
import org.apache.hadoop.hive.ql.anon.index.BtreeIndexReader;
import org.apache.hadoop.hive.ql.anon.index.IndexReader;
import org.apache.hadoop.hive.ql.anon.index.dir.DirectoryIndexReader;
import org.apache.hadoop.hive.ql.anon.index.tab.TabularIndexReader;
import org.apache.hadoop.hive.ql.anon.policy.DataErasurePolicy;
import org.apache.hadoop.hive.ql.io.HiveKey;
import org.apache.hadoop.io.BytesWritable;
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
import org.apache.tez.runtime.library.api.KeyValueWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.apache.hadoop.hive.ql.anon.consts.AnonConst.*;
import static org.apache.hadoop.hive.ql.anon.utils.Utils.parsePidList;
import static org.apache.hadoop.hive.ql.anon.utils.Utils.writableToBytes;
public class IndexProcessor extends AbstractLogicalIOProcessor {

  private static final Logger LOG = LoggerFactory.getLogger(IndexProcessor.class);
  private Configuration conf;
  private IndexType indexType;

  public IndexProcessor(final ProcessorContext context) {
    super(context);
  }

  @Override
  public void initialize() throws Exception {
    conf = TezUtils.createConfFromUserPayload(getContext().getUserPayload());
    final String ixType = conf.get(ANON_INDEX_TYPE);
    indexType = IndexType.valueOf(ixType);
  }

  @Override
  public void handleEvents(final List<Event> processorEvents) {

  }

  @Override
  public void close() throws Exception {

  }

  @Override
  public void run(final Map<String, LogicalInput> inputs, final Map<String, LogicalOutput> outputs) throws Exception {
    LOG.info("running index processor");
    for (LogicalInput input : inputs.values()) {
      input.start();
    }
    for (LogicalOutput output : outputs.values()) {
      output.start();
    }

    final KeyValueReader kvIndexListReader = (KeyValueReader) inputs.get(
        AnonConst.ANON_EDGE_IDX_IN).getReader();
    if (outputs.size() != 1) {
      throw new IllegalStateException("IndexProcessor expected exactly one output edge; got "
          + outputs.keySet());
    }
    final KeyValueWriter kvAnonWriter =
        (KeyValueWriter) outputs.values().iterator().next().getWriter();

    final String pidCsv = conf.get(ANON_PID_LIST);
    final String policyDoc = conf.get(ANON_POLICY_DOC);
    final DataErasurePolicy erasurePolicy = DataErasurePolicy.fromString(policyDoc);
    final List<WritableComparable> keys = parsePidList(pidCsv, erasurePolicy.identityFieldType);
    if (keys.isEmpty()) {
      return;
    }

    while (kvIndexListReader.next()) {
      final Object value = kvIndexListReader.getCurrentValue();
      final Text indexPath = (Text) value;
      final IndexReader indexReader = IndexReaderFactory.getIndexReader(indexType, conf, indexPath.toString());
      try {
        final BytesWritable bw = IndexResultConverter.convert(indexReader, keys);
        final HiveKey hiveKey = new HiveKey();
        kvAnonWriter.write(hiveKey, bw);
      } finally {
        if (indexReader instanceof Closeable) {
          ((Closeable) indexReader).close();
        }
      }
    }
  }
}
