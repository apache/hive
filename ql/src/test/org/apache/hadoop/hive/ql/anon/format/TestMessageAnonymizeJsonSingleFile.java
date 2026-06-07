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

package org.apache.hadoop.hive.ql.anon.format;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.crunch.types.orc.OrcUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.ColumnInternalFormat;
import org.apache.hadoop.hive.ql.anon.ConstCode;
import org.apache.hadoop.hive.ql.anon.anonymize.RowAnonymizer;
import org.apache.hadoop.hive.ql.anon.btree.LocatorSchemaItem;
import org.apache.hadoop.hive.ql.anon.btree.StructValueList;
import org.apache.hadoop.hive.ql.anon.btree.ValueItem;
import org.apache.hadoop.hive.ql.anon.convert.JsonBodyConverter;
import org.apache.hadoop.hive.ql.anon.extract.Extractor;
import org.apache.hadoop.hive.ql.anon.extract.MessageExtractor;
import org.apache.hadoop.hive.ql.anon.index.BtreeIndexReader;
import org.apache.hadoop.hive.ql.anon.index.IndexReader;
import org.apache.hadoop.hive.ql.anon.index.dir.DirectoryIndexReader;
import org.apache.hadoop.hive.ql.anon.index.tab.TabularIndexReader;
import org.apache.hadoop.hive.ql.anon.model.BaseMsg;
import org.apache.hadoop.hive.ql.anon.model.Msg3;
import org.apache.hadoop.hive.ql.anon.policy.DataErasurePolicy;
import org.apache.hadoop.hive.ql.anon.tez.Stats;
import org.apache.hadoop.hive.ql.anon.utils.FileUtils;
import org.apache.hadoop.hive.ql.anon.utils.MessageUtils;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.hadoop.hive.ql.io.orc.RecordReader;
import org.apache.hadoop.hive.ql.io.orc.Writer;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import static org.apache.hadoop.hive.ql.anon.TestUtils.getTestPolicy;
import static org.apache.hadoop.hive.ql.anon.consts.AnonConst.*;
import static org.apache.hadoop.hive.ql.anon.consts.BtreeConst.*;
import static org.apache.hadoop.hive.ql.anon.consts.MsgConst.MSG_MSG_1;
import static org.apache.hadoop.hive.ql.anon.consts.MsgConst.MSG_MSG_3;
public class TestMessageAnonymizeJsonSingleFile {

  private final Configuration conf = new Configuration();
  private final String orcStruct = "struct<m:int, o:bigint, b:string>";
  public static final ObjectMapper mapper = new ObjectMapper();
  private RowAnonymizer anonymizer;
  private Extractor extractor;
  private final IntWritable msgId = new IntWritable();
  private final LongWritable offset = new LongWritable();
  private final Text bodyColumn = new Text();
  private final TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(orcStruct);

  final int numAllMessages = 100_000_000;
  final int allToPiiRatio = 100;
  final int numPiiMessages = numAllMessages / allToPiiRatio;
  final int numDistinctUsers = 10_000;
  final int messagesPerUser = numAllMessages / numDistinctUsers;

  int userCounter = 0;
  int keyCount = 1;
  int piiMsgFieldLen = 30;
  int fileCount = 1;

  private String prefix = System.getProperty("java.io.tmpdir") + "/single_file_5_30";
  private Path path = new Path(prefix + "test_json.orc");
  private Path outputPath = new Path(prefix + "test_json_out.orc");
  private String indexFilePath = prefix + "000000_0.bt";
  private String dirIndexFilePath = prefix + "000000_0.dir";
  private String tabIndexFilePath = prefix + "000000_0.tab";
  private ConstCode formatCode = ConstCode.j;

  int msgIdIx = 0;
  int msgOffsetIx = 1;
  int bodyIx = 2;

  public TestMessageAnonymizeJsonSingleFile() {
    conf.set(ANON_COLUMN_INTERNAL_FORMAT, ColumnInternalFormat.JSON.name());
  }

  public void testGenFileJson() throws IOException {

    ObjectInspector oi = OrcStruct.createObjectInspector(typeInfo);
    if (FileUtils.fileExists(path, conf)) {
      FileUtils.deleteFile(path, conf);
    }
    Writer writer = FileUtils.getWriter(path, conf, oi);

    System.out.printf("msg count: %,d, user count: %,d", numAllMessages, numDistinctUsers);

    for (int i = 0; i < numAllMessages; i++) {

      OrcStruct row = getRow(i);
      writer.addRow(row);
    }

    writer.writeIntermediateFooter();
    writer.close();
  }

  private OrcStruct getRow(int i) throws JsonProcessingException {
    BaseMsg msg = null;
    int ratio1 = numAllMessages / numPiiMessages;
    if (i % ratio1 == 0) {
      int userId = userCounter % numDistinctUsers;
      msgId.set(MSG_MSG_3);
      msg = MessageUtils.createMsg3(userId, piiMsgFieldLen);
      userCounter++;
    } else {
      msgId.set(MSG_MSG_1);
      msg = MessageUtils.createMsg1(i);
    }

    String json = mapper.writeValueAsString(msg);
    bodyColumn.set(json);
    offset.set(i);
    return OrcUtils.createOrcStruct(typeInfo, msgId, offset, bodyColumn);
  }

  public void testReadOnlyJson() throws IOException {
    final Stats stats = new Stats();
    long start = System.currentTimeMillis();
    final Reader reader = OrcFile.createReader(path, OrcFile.readerOptions(conf));

    final RecordReader recordReader = reader.rows();
    Object rowIn = null;
    while (recordReader.hasNext()) {
      rowIn = recordReader.next(rowIn);
      Object rowOut = rowIn;
    }

    recordReader.close();
    final long end = System.currentTimeMillis();
    stats.totalProcessingTime = end - start;
    System.out.printf("(RdWrAnTab) %s\n", stats);
  }

  public void testReadWriteJson() throws IOException {

    if (FileUtils.fileExists(outputPath, conf)) {
      FileUtils.deleteFile(outputPath, conf);
    }

    long start = System.currentTimeMillis();
    Reader reader = OrcFile.createReader(path, OrcFile.readerOptions(conf));
    StructObjectInspector inputSOI = (StructObjectInspector) reader.getObjectInspector();
    Writer writer = FileUtils.getWriter(outputPath, conf, inputSOI);

    RecordReader recordReader = reader.rows();
    Object rowIn = null;
    while (recordReader.hasNext()) {
      rowIn = recordReader.next(rowIn);
      Object rowOut = processFileRow(rowIn, inputSOI);
      writer.addRow(rowOut);
    }

    writer.writeIntermediateFooter();
    writer.close();
    recordReader.close();
    long end = System.currentTimeMillis();
    long diff = end - start;
    System.out.printf("%,d\n", diff);
  }

  private Object processFileRow(final Object rowIn, final StructObjectInspector soi) {
    List<Object> values = soi.getStructFieldsDataAsList(rowIn);
    return rowIn;
  }

  public void testReadAnonymizeJson() throws IOException {
    conf.set(ANON_COLUMN_INTERNAL_FORMAT, ColumnInternalFormat.JSON.name());
    DataErasurePolicy policy = getTestPolicy();
    anonymizer = new RowAnonymizer(conf, policy);
    extractor = new MessageExtractor(formatCode);

    final Stats stats = new Stats();

    List<WritableComparable> keys = getKeys(keyCount);

    long start = System.currentTimeMillis();
    Reader reader = OrcFile.createReader(path, OrcFile.readerOptions(conf));
    StructObjectInspector inputSOI = (StructObjectInspector) reader.getObjectInspector();

    RecordReader recordReader = reader.rows();
    Object rowIn = null;
    while (recordReader.hasNext()) {
      rowIn = recordReader.next(rowIn);
      Object rowOut = processFileRow(rowIn, inputSOI, keys, stats);
      if (rowOut == null) {
        rowOut = rowIn;
      }
    }

    recordReader.close();

    long end = System.currentTimeMillis();
    stats.totalProcessingTime = end - start;
    System.out.printf("(RdAn) %s\n", stats);
  }

  public void testReadWriteAnonymizeJson() throws IOException {
    conf.set(ANON_COLUMN_INTERNAL_FORMAT, ColumnInternalFormat.JSON.name());

    DataErasurePolicy policy = getTestPolicy();
    anonymizer = new RowAnonymizer(conf, policy);
    extractor = new MessageExtractor(formatCode);

    final Stats stats = new Stats();

    List<WritableComparable> keys = getKeys(keyCount);

    if (FileUtils.fileExists(outputPath, conf)) {
      FileUtils.deleteFile(outputPath, conf);
    }

    long start = System.currentTimeMillis();
    Reader reader = OrcFile.createReader(path, OrcFile.readerOptions(conf));
    StructObjectInspector inputSOI = (StructObjectInspector) reader.getObjectInspector();
    Writer writer = FileUtils.getWriter(outputPath, conf, inputSOI);

    RecordReader recordReader = reader.rows();
    Object rowIn = null;
    while (recordReader.hasNext()) {
      rowIn = recordReader.next(rowIn);
      Object rowOut = processFileRow(rowIn, inputSOI, keys, stats);
      if (rowOut == null) {
        rowOut = rowIn;
      }
      writer.addRow(rowOut);
    }

    writer.writeIntermediateFooter();
    writer.close();
    recordReader.close();

    long end = System.currentTimeMillis();
    stats.totalProcessingTime = end - start;
    System.out.printf("(RdWrAn) %s\n", stats);
  }

  private Object processFileRow(final Object rowIn, final StructObjectInspector soi, final List<WritableComparable> keys, final Stats stats) {
    stats.visitedMessages++;

    List<Object> values = soi.getStructFieldsDataAsList(rowIn);
    WritableComparable msgId = (WritableComparable) values.get(msgIdIx);
    Writable body = (Writable) values.get(bodyIx);
    if (!anonymizer.matches(msgId)) {
      return rowIn;
    }
    stats.policyMessages++;

    if (!extractor.containsIdentityField(IDENTITY_FIELD_NAME, msgId, body)) {
      return rowIn;
    }
    stats.piiMessages++;

    Set<WritableComparable> set = new HashSet<>();
    extractor.extractIdentifyFieldValues(new Text(IDENTITY_FIELD_NAME), msgId, body, set);
    boolean found = false;
    for (WritableComparable key : keys) {
      if (set.contains(key)) {
        found = true;
        break;
      }
    }

    if (!found) {
      return rowIn;
    }

    long start = System.currentTimeMillis();
    Writable anonymized = anonymizer.anonymize(msgId, body);
    values.set(bodyIx, anonymized);
    Object obj = OrcUtils.createOrcStruct(typeInfo, values.toArray());
    long end = System.currentTimeMillis();

    stats.totalAnonTime += (end - start);
    stats.anonymizedMessages++;
    return obj;
  }

  public void testReadWriteAnonymizeWithBtreeIndexJson() throws IOException {
    conf.set(ANON_COLUMN_INTERNAL_FORMAT, ColumnInternalFormat.JSON.name());

    DataErasurePolicy policy = getTestPolicy();
    anonymizer = new RowAnonymizer(conf, policy);
    extractor = new MessageExtractor(formatCode);

    List<WritableComparable> keys = getKeys(keyCount);

    if (FileUtils.fileExists(outputPath, conf)) {
      FileUtils.deleteFile(outputPath, conf);
    }

    final Stats stats = new Stats();
    long start = System.currentTimeMillis();
    Reader reader = OrcFile.createReader(path, OrcFile.readerOptions(conf));
    StructObjectInspector inputSOI = (StructObjectInspector) reader.getObjectInspector();
    Writer writer = FileUtils.getWriter(outputPath, conf, inputSOI);

    MapWritable mw = getMapFromBtree(keys);
    AtomicReference<MapWritable> locToSchema = new AtomicReference<>(new MapWritable());
    mw.forEach((file, locatorToSchema) -> {
      locToSchema.set((MapWritable) locatorToSchema);
    });
    MapWritable l2schema = locToSchema.get();
    long end1 = System.currentTimeMillis();

    RecordReader recordReader = reader.rows();
    Object rowIn = null;
    while (recordReader.hasNext()) {
      rowIn = recordReader.next(rowIn);
      Object rowOut = processFileRow(rowIn, inputSOI, l2schema, stats);
      if (rowOut == null) {
        rowOut = rowIn;
      }
      writer.addRow(rowOut);
    }

    writer.writeIntermediateFooter();
    writer.close();
    recordReader.close();

    long end = System.currentTimeMillis();
    stats.totalProcessingTime = end - start;
    stats.seekTime = end1 - start;
    System.out.printf("(RdWrAnBt) %s\n", stats);
  }

  public void testReadWriteAnonymizeWithDirectoryIndexJson() throws IOException {
    conf.set(ANON_COLUMN_INTERNAL_FORMAT, ColumnInternalFormat.JSON.name());

    DataErasurePolicy policy = getTestPolicy();
    anonymizer = new RowAnonymizer(conf, policy);
    extractor = new MessageExtractor(formatCode);

    List<WritableComparable> keys = getKeys(keyCount);

    if (FileUtils.fileExists(outputPath, conf)) {
      FileUtils.deleteFile(outputPath, conf);
    }

    final Stats stats = new Stats();
    long start = System.currentTimeMillis();
    Reader reader = OrcFile.createReader(path, OrcFile.readerOptions(conf));
    StructObjectInspector inputSOI = (StructObjectInspector) reader.getObjectInspector();
    Writer writer = FileUtils.getWriter(outputPath, conf, inputSOI);

    MapWritable mw = getMapFromDirectory(keys);
    AtomicReference<MapWritable> locToSchema = new AtomicReference<>(new MapWritable());
    mw.forEach((file, locatorToSchema) -> {
      locToSchema.set((MapWritable) locatorToSchema);
    });
    MapWritable l2schema = locToSchema.get();
    long end1 = System.currentTimeMillis();

    RecordReader recordReader = reader.rows();
    Object rowIn = null;
    while (recordReader.hasNext()) {
      rowIn = recordReader.next(rowIn);
      Object rowOut = processFileRow(rowIn, inputSOI, l2schema, stats);
      if (rowOut == null) {
        rowOut = rowIn;
      }
      writer.addRow(rowOut);
    }

    writer.writeIntermediateFooter();
    writer.close();
    recordReader.close();

    long end = System.currentTimeMillis();
    stats.totalProcessingTime = end - start;
    stats.seekTime = end1 - start;
    System.out.printf("(RdWrAnLkp) %s\n", stats);
  }

  public void testReadWriteAnonymizeWithTabularIndexJson() throws IOException {
    conf.set(ANON_COLUMN_INTERNAL_FORMAT, ColumnInternalFormat.JSON.name());

    DataErasurePolicy policy = getTestPolicy();
    anonymizer = new RowAnonymizer(conf, policy);
    extractor = new MessageExtractor(formatCode);

    List<WritableComparable> keys = getKeys(keyCount);

    if (FileUtils.fileExists(outputPath, conf)) {
      FileUtils.deleteFile(outputPath, conf);
    }

    final Stats stats = new Stats();
    long start = System.currentTimeMillis();
    Reader reader = OrcFile.createReader(path, OrcFile.readerOptions(conf));
    StructObjectInspector inputSOI = (StructObjectInspector) reader.getObjectInspector();
    Writer writer = FileUtils.getWriter(outputPath, conf, inputSOI);

    MapWritable mw = getMapFromTabular(keys);
    AtomicReference<MapWritable> locToSchema = new AtomicReference<>(new MapWritable());
    mw.forEach((file, locatorToSchema) -> {
      locToSchema.set((MapWritable) locatorToSchema);
    });
    MapWritable l2schema = locToSchema.get();
    long end1 = System.currentTimeMillis();

    RecordReader recordReader = reader.rows();
    Object rowIn = null;
    while (recordReader.hasNext()) {
      rowIn = recordReader.next(rowIn);
      Object rowOut = processFileRow(rowIn, inputSOI, l2schema, stats);
      if (rowOut == null) {
        rowOut = rowIn;
      }
      writer.addRow(rowOut);
    }

    writer.writeIntermediateFooter();
    writer.close();
    recordReader.close();

    long end = System.currentTimeMillis();
    stats.totalProcessingTime = end - start;
    stats.seekTime = end1 - start;
    System.out.printf("(RdWrAnTab) %s\n", stats);
  }

  public void testIxRead() throws IOException {
    List<WritableComparable> keys = getKeys(20);
    MapWritable ixResult = getMapFromBtree(keys);
  }

  private MapWritable getMapFromBtree(List<WritableComparable> keys) throws IOException {
    conf.setInt(BTREE_CONF_BUFFER_SIZE, 10 * 8192);
    conf.setInt(BTREE_CONF_PAGE_SIZE, 8192);
    conf.setInt(BTREE_CONF_PAGE_HEADER_SIZE, 64);
    conf.setInt(BTREE_CONF_DUMP_BYTES_PER_ROW, 8192);
    conf.set(INDEX_ADDR_TYPE, "I");
    conf.set(INDEX_KEY_TYPE, "I");
    conf.set(INDEX_VALUE_TYPES, ANON_INDEX_TEST_VALUE_TYPES);
    IndexReader reader = new BtreeIndexReader(conf, indexFilePath);

    MapWritable indexResult = new MapWritable();
    for (WritableComparable key : keys) {
      Writable result = reader.seek(key);
      if (result == null) {
        continue;
      }
      StructValueList valueList = (StructValueList) result;
      for (ValueItem valueItem : valueList.getItems()) {
        Writable file = valueItem.filePath;
        if (!indexResult.containsKey(file)) {
          indexResult.put(file, new MapWritable());
        }

        MapWritable locatorToSchema = (MapWritable) indexResult.get(file);
        for (LocatorSchemaItem lsi : valueItem.getItemList()) {
          locatorToSchema.put(lsi.rowLocator, lsi.schemaId);
        }
      }
    }

    return indexResult;
  }

  private MapWritable getMapFromDirectory(List<WritableComparable> keys) throws IOException {
    conf.setInt(BTREE_CONF_BUFFER_SIZE, 10 * 8192);
    conf.setInt(BTREE_CONF_PAGE_SIZE, 8192);
    conf.setInt(BTREE_CONF_PAGE_HEADER_SIZE, 64);
    conf.setInt(BTREE_CONF_DUMP_BYTES_PER_ROW, 8192);
    conf.set(INDEX_ADDR_TYPE, "I");
    conf.set(INDEX_KEY_TYPE, "I");
    conf.set(INDEX_VALUE_TYPES, ANON_INDEX_TEST_VALUE_TYPES);
    IndexReader reader = new DirectoryIndexReader(conf, dirIndexFilePath);

    MapWritable indexResult = new MapWritable();
    for (WritableComparable key : keys) {
      Writable result = reader.seek(key);
      if (result == null) {
        continue;
      }
      StructValueList valueList = (StructValueList) result;
      for (ValueItem valueItem : valueList.getItems()) {
        Writable file = valueItem.filePath;
        if (!indexResult.containsKey(file)) {
          indexResult.put(file, new MapWritable());
        }

        MapWritable locatorToSchema = (MapWritable) indexResult.get(file);
        for (LocatorSchemaItem lsi : valueItem.getItemList()) {
          locatorToSchema.put(lsi.rowLocator, lsi.schemaId);
        }
      }
    }

    return indexResult;
  }

  private MapWritable getMapFromTabular(List<WritableComparable> keys) throws IOException {
    conf.setInt(BTREE_CONF_BUFFER_SIZE, 10 * 8192);
    conf.setInt(BTREE_CONF_PAGE_SIZE, 8192);
    conf.setInt(BTREE_CONF_PAGE_HEADER_SIZE, 64);
    conf.setInt(BTREE_CONF_DUMP_BYTES_PER_ROW, 8192);
    conf.set(INDEX_ADDR_TYPE, "I");
    conf.set(INDEX_KEY_TYPE, "I");
    conf.set(INDEX_VALUE_TYPES, ANON_INDEX_TEST_VALUE_TYPES);
    IndexReader reader = new TabularIndexReader(conf, tabIndexFilePath);

    MapWritable indexResult = new MapWritable();
    for (WritableComparable key : keys) {
      Writable result = reader.seek(key);
      if (result == null) {
        continue;
      }
      StructValueList valueList = (StructValueList) result;
      for (ValueItem valueItem : valueList.getItems()) {
        Writable file = valueItem.filePath;
        if (!indexResult.containsKey(file)) {
          indexResult.put(file, new MapWritable());
        }

        MapWritable locatorToSchema = (MapWritable) indexResult.get(file);
        for (LocatorSchemaItem lsi : valueItem.getItemList()) {
          locatorToSchema.put(lsi.rowLocator, lsi.schemaId);
        }
      }
    }

    return indexResult;
  }

  private Object processFileRow(final Object rowIn, final StructObjectInspector soi, final MapWritable locatorToSchema, final Stats stats) {
    stats.visitedMessages++;

    List<Object> values = soi.getStructFieldsDataAsList(rowIn);
    WritableComparable offset = (WritableComparable) values.get(msgOffsetIx);
    if (!locatorToSchema.containsKey(offset)) {
      return rowIn;
    } else {
      long start = System.currentTimeMillis();

      Writable body = (Writable) values.get(bodyIx);
      WritableComparable msgId = (WritableComparable) values.get(msgIdIx);
      Writable anonymized = anonymizer.anonymize(msgId, body);
      values.set(bodyIx, anonymized);
      Object obj = OrcUtils.createOrcStruct(typeInfo, values.toArray());

      long end = System.currentTimeMillis();
      stats.totalAnonTime += (end - start);
      stats.anonymizedMessages++;
      return obj;
    }
  }

  public void testAnonFile() throws IOException {
    long start = System.currentTimeMillis();
    Reader reader = OrcFile.createReader(outputPath, OrcFile.readerOptions(conf));
    StructObjectInspector inputSOI = (StructObjectInspector) reader.getObjectInspector();

    JsonBodyConverter converter = new JsonBodyConverter();

    IntWritable m3 = new IntWritable(3);
    RecordReader recordReader = reader.rows();
    Object rowIn = null;
    while (recordReader.hasNext()) {
      rowIn = recordReader.next(rowIn);
      OrcStruct struct = (OrcStruct) rowIn;
      Object o = struct.getFieldValue(2);

      List<Object> values = inputSOI.getStructFieldsDataAsList(rowIn);
      WritableComparable msgId = (WritableComparable) values.get(msgIdIx);
      Writable body = (Writable) values.get(bodyIx);
      String json = body.toString();
      int jsonLen = json.length();

      if (msgId.compareTo(m3) != 0) {
        continue;
      }

      BaseMsg msg = converter.convertBody(msgId, body);
      Msg3 msg3 = (Msg3) msg;
      if (msg3.getUserId() == 0) {
        System.out.println(msg3.getCountry());
      }
    }

    recordReader.close();
    long end = System.currentTimeMillis();
    long diff = end - start;
    System.out.printf("read file: %,d", diff);
  }

  private List<WritableComparable> getKeys(int count) {
    if (count > numDistinctUsers) {
      throw new IllegalArgumentException("count > numDistinctUsers");
    }
    List<WritableComparable> keys = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      keys.add(new IntWritable(i));
    }
    return keys;
  }

  public static void main(String[] args) throws IOException {
    if (args.length == 0) {
      return;
    }
    int n = Integer.parseInt(args[0]);
    TestMessageAnonymizeJsonSingleFile tester = new TestMessageAnonymizeJsonSingleFile();
    tester.keyCount = n;
    tester.testReadWriteAnonymizeJson();
  }
}
