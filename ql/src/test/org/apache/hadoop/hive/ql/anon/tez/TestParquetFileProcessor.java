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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.ColumnInternalFormat;
import org.apache.hadoop.hive.ql.anon.ConstCode;
import org.apache.hadoop.hive.ql.anon.anonymize.RowAnonymizer;
import org.apache.hadoop.hive.ql.anon.extract.Extractor;
import org.apache.hadoop.hive.ql.anon.extract.MessageExtractor;
import org.apache.hadoop.hive.ql.anon.model.BaseMsg;
import org.apache.hadoop.hive.ql.anon.policy.DataErasurePolicy;
import org.apache.hadoop.hive.ql.anon.utils.FileUtils;
import org.apache.hadoop.hive.ql.anon.utils.MessageUtils;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.io.IOConstants;
import org.apache.hadoop.hive.serde2.io.ParquetHiveRecord;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.RecordReader;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.hive.ql.anon.consts.AnonConst.ANON_COLUMN_INTERNAL_FORMAT;
import static org.apache.hadoop.hive.ql.anon.consts.AnonConst.IDENTITY_FIELD_NAME;
import static org.apache.hadoop.hive.ql.anon.consts.MsgConst.MSG_MSG_1;
import static org.apache.hadoop.hive.ql.anon.consts.MsgConst.MSG_MSG_3;
import static org.apache.hadoop.hive.ql.anon.TestUtils.getTestPolicy;

public class TestParquetFileProcessor {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final int FIELD_LEN = 20;
  private static final String ERASED_COUNTRY = "\"country\":\"\"";

  private static final int MSG_ID_IX = 0;
  private static final int OFFSET_IX = 1;
  private static final int BODY_IX = 2;

  private Configuration newConf() {
    final Configuration conf = new Configuration();
    conf.set("fs.defaultFS", "file:///");
    conf.set(ANON_COLUMN_INTERNAL_FORMAT, ColumnInternalFormat.JSON.name());
    conf.set(IOConstants.COLUMNS, "m,o,b");
    conf.set(IOConstants.COLUMNS_TYPES, "int,bigint,string");
    return conf;
  }

  @Test
  public void noIndexAnonymisesOnlyKeyedRows(@TempDir final java.nio.file.Path tmp) throws IOException {
    final Configuration conf = newConf();
    final Path dataPath = new Path(tmp.resolve("data.parquet").toString());

    final int[] piiUsers = {0, 1, 2, 3, 4};
    final long[] msg1Offsets = {5, 6};

    final FileSinkOperator.RecordWriter writer = FileUtils.getWriter(dataPath, conf);
    final ParquetHiveRecord record = new ParquetHiveRecord();
    record.inspector = FileUtils.getOI(conf);
    for (final int user : piiUsers) {
      final BaseMsg m = MessageUtils.createMsg3(user, FIELD_LEN);
      record.value = row(MSG_MSG_3, user, MAPPER.writeValueAsString(m));
      writer.write(record);
    }
    for (final long off : msg1Offsets) {
      final BaseMsg m = MessageUtils.createMsg1(off);
      record.value = row(MSG_MSG_1, off, MAPPER.writeValueAsString(m));
      writer.write(record);
    }
    writer.close(false);

    final List<WritableComparable> keys = new ArrayList<>();
    keys.add(new IntWritable(0));
    keys.add(new IntWritable(1));

    final DataErasurePolicy policy = getTestPolicy();
    final RowAnonymizer anonymizer = new RowAnonymizer(conf, policy);
    final Extractor extractor = new MessageExtractor(ConstCode.j);
    final RowContext rowContext =
        new RowContext(MSG_ID_IX, OFFSET_IX, BODY_IX, IDENTITY_FIELD_NAME, ColumnInternalFormat.JSON);

    final ParquetFileProcessor processor =
        new ParquetFileProcessor(conf, extractor, anonymizer, rowContext, keys);
    processor.processFile(new AnonContext(dataPath));

    final Map<Long, String> body = new HashMap<>();
    final RecordReader<NullWritable, ArrayWritable> reader = FileUtils.getReader(dataPath, conf);
    final NullWritable rk = reader.createKey();
    final ArrayWritable rv = reader.createValue();
    int rows = 0;
    while (reader.next(rk, rv)) {
      final Writable[] v = rv.get();
      body.put(((LongWritable) v[OFFSET_IX]).get(), String.valueOf(v[BODY_IX]));
      rows++;
    }
    reader.close();

    Assertions.assertEquals(piiUsers.length + msg1Offsets.length, rows, "row count must be preserved");

    Assertions.assertTrue(body.get(0L).contains(ERASED_COUNTRY), "user 0 (keyed) country must be erased");
    Assertions.assertTrue(body.get(1L).contains(ERASED_COUNTRY), "user 1 (keyed) country must be erased");
    for (final long off : new long[] {2, 3, 4}) {
      Assertions.assertTrue(body.get(off).contains("\"country\":\""), "offset " + off + " must have a country field");
      Assertions.assertFalse(body.get(off).contains(ERASED_COUNTRY),
          "offset " + off + " (non-keyed) country must be preserved");
    }

    final Stats stats = processor.getStats();
    Assertions.assertEquals(7, stats.visitedMessages, "every row is visited");
    Assertions.assertEquals(5, stats.piiMessages, "all five Msg3 rows carry the identity field");
    Assertions.assertEquals(2, stats.anonymizedMessages, "only the two keyed users are erased");
  }

  private Writable row(final int msgId, final long offset, final String bodyJson) {
    final Writable[] cells = new Writable[3];
    cells[MSG_ID_IX] = new IntWritable(msgId);
    cells[OFFSET_IX] = new LongWritable(offset);
    cells[BODY_IX] = new Text(bodyJson);
    final ArrayWritable aw = new ArrayWritable(Writable.class);
    aw.set(cells);
    return aw;
  }
}
