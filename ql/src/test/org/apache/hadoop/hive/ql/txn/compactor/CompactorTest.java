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
package org.apache.hadoop.hive.ql.txn.compactor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.MetaStoreThread;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hadoop.hive.metastore.txn.CompactionTxnHandler;
import org.apache.hadoop.hive.metastore.txn.TxnDbUtil;
import org.apache.hadoop.hive.ql.io.AcidInputFormat;
import org.apache.hadoop.hive.ql.io.AcidOutputFormat;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.io.FSRecordWriter;
import org.apache.hadoop.hive.ql.io.RecordIdentifier;
import org.apache.hadoop.hive.ql.io.RecordUpdater;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.Progressable;
import org.apache.thrift.TException;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Stack;

/**
 * Super class for all of the compactor test modules.
 */
public abstract class CompactorTest {
  static final private String CLASS_NAME = CompactorTest.class.getName();
  static final private Log LOG = LogFactory.getLog(CLASS_NAME);

  protected CompactionTxnHandler txnHandler;
  protected IMetaStoreClient ms;
  protected long sleepTime = 1000;

  private MetaStoreThread.BooleanPointer stop = new MetaStoreThread.BooleanPointer();
  private File tmpdir;

  protected CompactorTest() throws Exception {
    HiveConf conf = new HiveConf();
    TxnDbUtil.setConfValues(conf);
    TxnDbUtil.cleanDb();
    ms = new HiveMetaStoreClient(conf);
    txnHandler = new CompactionTxnHandler(conf);
    tmpdir = new File(System.getProperty("java.io.tmpdir") +
        System.getProperty("file.separator") + "compactor_test_tables");
    tmpdir.mkdir();
    tmpdir.deleteOnExit();
  }

  protected void startInitiator(HiveConf conf) throws Exception {
    startThread('i', conf);
  }

  protected void startWorker(HiveConf conf) throws Exception {
    startThread('w', conf);
  }

  protected void startCleaner(HiveConf conf) throws Exception {
    startThread('c', conf);
  }

  protected Table newTable(String dbName, String tableName, boolean partitioned) throws TException {
    return newTable(dbName, tableName, partitioned, new HashMap<String, String>(), null);
  }

  protected Table newTable(String dbName, String tableName, boolean partitioned,
                           Map<String, String> parameters)  throws TException {
    return newTable(dbName, tableName, partitioned, parameters, null);

  }

  protected Table newTable(String dbName, String tableName, boolean partitioned,
                           Map<String, String> parameters, List<Order> sortCols)
      throws  TException {
    Table table = new Table();
    table.setTableName(tableName);
    table.setDbName(dbName);
    table.setOwner("me");
    table.setSd(newStorageDescriptor(getLocation(tableName, null), sortCols));
    List<FieldSchema> partKeys = new ArrayList<FieldSchema>(1);
    if (partitioned) {
      partKeys.add(new FieldSchema("ds", "string", "no comment"));
      table.setPartitionKeys(partKeys);
    }

    table.setParameters(parameters);

    ms.createTable(table);
    return table;
  }

  protected Partition newPartition(Table t, String value) throws Exception {
    return newPartition(t, value, null);
  }

  protected Partition newPartition(Table t, String value, List<Order> sortCols) throws Exception {
    Partition part = new Partition();
    part.addToValues(value);
    part.setDbName(t.getDbName());
    part.setTableName(t.getTableName());
    part.setSd(newStorageDescriptor(getLocation(t.getTableName(), value), sortCols));
    part.setParameters(new HashMap<String, String>());
    ms.add_partition(part);
    return part;
  }

  protected long openTxn() throws MetaException {
    List<Long> txns = txnHandler.openTxns(new OpenTxnRequest(1, System.getProperty("user.name"),
        Worker.hostname())).getTxn_ids();
    return txns.get(0);
  }

  protected void addDeltaFile(HiveConf conf, Table t, Partition p, long minTxn, long maxTxn,
                              int numRecords) throws Exception{
    addFile(conf, t, p, minTxn, maxTxn, numRecords, FileType.DELTA, 2, true);
  }

  protected void addBaseFile(HiveConf conf, Table t, Partition p, long maxTxn,
                             int numRecords) throws Exception{
    addFile(conf, t, p, 0, maxTxn, numRecords, FileType.BASE, 2, true);
  }

  protected void addLegacyFile(HiveConf conf, Table t, Partition p,
                               int numRecords) throws Exception {
    addFile(conf, t, p, 0, 0, numRecords, FileType.LEGACY, 2, true);
  }

  protected void addDeltaFile(HiveConf conf, Table t, Partition p, long minTxn, long maxTxn,
                              int numRecords, int numBuckets, boolean allBucketsPresent)
      throws Exception {
    addFile(conf, t, p, minTxn, maxTxn, numRecords, FileType.DELTA, numBuckets, allBucketsPresent);
  }

  protected void addBaseFile(HiveConf conf, Table t, Partition p, long maxTxn,
                             int numRecords, int numBuckets, boolean allBucketsPresent)
      throws Exception {
    addFile(conf, t, p, 0, maxTxn, numRecords, FileType.BASE, numBuckets, allBucketsPresent);
  }

  protected void addLegacyFile(HiveConf conf, Table t, Partition p,
                               int numRecords, int numBuckets, boolean allBucketsPresent)
      throws Exception {
    addFile(conf, t, p, 0, 0, numRecords, FileType.LEGACY, numBuckets, allBucketsPresent);
  }

  protected List<Path> getDirectories(HiveConf conf, Table t, Partition p) throws Exception {
    String partValue = (p == null) ? null : p.getValues().get(0);
    String location = getLocation(t.getTableName(), partValue);
    Path dir = new Path(location);
    FileSystem fs = FileSystem.get(conf);
    FileStatus[] stats = fs.listStatus(dir);
    List<Path> paths = new ArrayList<Path>(stats.length);
    for (int i = 0; i < stats.length; i++) paths.add(stats[i].getPath());
    return paths;
  }

  protected void burnThroughTransactions(int num) throws MetaException, NoSuchTxnException, TxnAbortedException {
    OpenTxnsResponse rsp = txnHandler.openTxns(new OpenTxnRequest(num, "me", "localhost"));
    for (long tid : rsp.getTxn_ids()) txnHandler.commitTxn(new CommitTxnRequest(tid));
  }

  private StorageDescriptor newStorageDescriptor(String location, List<Order> sortCols) {
    StorageDescriptor sd = new StorageDescriptor();
    List<FieldSchema> cols = new ArrayList<FieldSchema>(2);
    cols.add(new FieldSchema("a", "varchar(25)", "still no comment"));
    cols.add(new FieldSchema("b", "int", "comment"));
    sd.setCols(cols);
    sd.setLocation(location);
    sd.setInputFormat(MockInputFormat.class.getName());
    sd.setOutputFormat(MockOutputFormat.class.getName());
    sd.setNumBuckets(1);
    SerDeInfo serde = new SerDeInfo();
    serde.setSerializationLib(LazySimpleSerDe.class.getName());
    sd.setSerdeInfo(serde);
    List<String> bucketCols = new ArrayList<String>(1);
    bucketCols.add("a");
    sd.setBucketCols(bucketCols);

    if (sortCols != null) {
      sd.setSortCols(sortCols);
    }
    return sd;
  }

  // I can't do this with @Before because I want to be able to control the config file provided
  // to each test.
  private void startThread(char type, HiveConf conf) throws Exception {
    TxnDbUtil.setConfValues(conf);
    CompactorThread t = null;
    switch (type) {
      case 'i': t = new Initiator(); break;
      case 'w': t = new Worker(); break;
      case 'c': t = new Cleaner(); break;
      default: throw new RuntimeException("Huh? Unknown thread type.");
    }
    t.setThreadId((int) t.getId());
    t.setHiveConf(conf);
    stop.boolVal = true;
    t.init(stop);
    t.run();
  }

  private String getLocation(String tableName, String partValue) {
    String location =  tmpdir.getAbsolutePath() +
      System.getProperty("file.separator") + tableName;
    if (partValue != null) {
      location += System.getProperty("file.separator") + "ds=" + partValue;
    }
    return location;
  }

  private enum FileType {BASE, DELTA, LEGACY};

  private void addFile(HiveConf conf, Table t, Partition p, long minTxn, long maxTxn,
                       int numRecords,  FileType type, int numBuckets,
                       boolean allBucketsPresent) throws Exception {
    String partValue = (p == null) ? null : p.getValues().get(0);
    Path location = new Path(getLocation(t.getTableName(), partValue));
    String filename = null;
    switch (type) {
      case BASE: filename = "base_" + maxTxn; break;
      case DELTA: filename = "delta_" + minTxn + "_" + maxTxn; break;
      case LEGACY: break; // handled below
    }

    FileSystem fs = FileSystem.get(conf);
    for (int bucket = 0; bucket < numBuckets; bucket++) {
      if (bucket == 0 && !allBucketsPresent) continue; // skip one
      Path partFile = null;
      if (type == FileType.LEGACY) {
        partFile = new Path(location, String.format(AcidUtils.BUCKET_DIGITS, bucket) + "_0");
      } else {
        Path dir = new Path(location, filename);
        fs.mkdirs(dir);
        partFile = AcidUtils.createBucketFile(dir, bucket);
      }
      FSDataOutputStream out = fs.create(partFile);
      for (int i = 0; i < numRecords; i++) {
        RecordIdentifier ri = new RecordIdentifier(maxTxn - 1, bucket, i);
        ri.write(out);
        out.writeBytes("mary had a little lamb its fleece was white as snow\n");
      }
      out.close();
    }
  }

  static class MockInputFormat implements AcidInputFormat<Text> {

    @Override
    public AcidInputFormat.RowReader<Text> getReader(InputSplit split,
                                                          Options options) throws
        IOException {
      return null;
    }

    @Override
    public RawReader<Text> getRawReader(Configuration conf, boolean collapseEvents, int bucket,
                                        ValidTxnList validTxnList,
                                        Path baseDirectory, Path... deltaDirectory) throws IOException {

      List<Path> filesToRead = new ArrayList<Path>();
      if (baseDirectory != null) {
        if (baseDirectory.getName().startsWith(AcidUtils.BASE_PREFIX)) {
          Path p = AcidUtils.createBucketFile(baseDirectory, bucket);
          FileSystem fs = p.getFileSystem(conf);
          if (fs.exists(p)) filesToRead.add(p);
        } else {
          filesToRead.add(new Path(baseDirectory, "00000_0"));

        }
      }
      for (int i = 0; i < deltaDirectory.length; i++) {
        Path p = AcidUtils.createBucketFile(deltaDirectory[i], bucket);
        FileSystem fs = p.getFileSystem(conf);
        if (fs.exists(p)) filesToRead.add(p);
      }
      return new MockRawReader(conf, filesToRead);
    }

    @Override
    public InputSplit[] getSplits(JobConf entries, int i) throws IOException {
      return new InputSplit[0];
    }

    @Override
    public RecordReader<NullWritable, Text> getRecordReader(InputSplit inputSplit, JobConf entries,
                                                            Reporter reporter) throws IOException {
      return null;
    }

    @Override
    public boolean validateInput(FileSystem fs, HiveConf conf, ArrayList<FileStatus> files) throws
        IOException {
      return false;
    }
  }

  static class MockRawReader implements AcidInputFormat.RawReader<Text> {
    private Stack<Path> filesToRead;
    private Configuration conf;
    private FSDataInputStream is = null;
    private FileSystem fs;

    MockRawReader(Configuration conf, List<Path> files) throws IOException {
      filesToRead = new Stack<Path>();
      for (Path file : files) filesToRead.push(file);
      this.conf = conf;
      fs = FileSystem.get(conf);
    }

    @Override
    public ObjectInspector getObjectInspector() {
      return null;
    }

    @Override
    public boolean next(RecordIdentifier identifier, Text text) throws IOException {
      if (is == null) {
        // Open the next file
        if (filesToRead.empty()) return false;
        Path p = filesToRead.pop();
        LOG.debug("Reading records from " + p.toString());
        is = fs.open(p);
      }
      String line = null;
      try {
        identifier.readFields(is);
        line = is.readLine();
      } catch (EOFException e) {
      }
      if (line == null) {
        // Set our current entry to null (since it's done) and try again.
        is = null;
        return next(identifier, text);
      }
      text.set(line);
      return true;
    }

    @Override
    public RecordIdentifier createKey() {
      return new RecordIdentifier();
    }

    @Override
    public Text createValue() {
      return new Text();
    }

    @Override
    public long getPos() throws IOException {
      return 0;
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public float getProgress() throws IOException {
      return 0;
    }
  }

  // This class isn't used and I suspect does totally the wrong thing.  It's only here so that I
  // can provide some output format to the tables and partitions I create.  I actually write to
  // those tables directory.
  static class MockOutputFormat implements AcidOutputFormat<Text> {

    @Override
    public RecordUpdater getRecordUpdater(Path path, Options options) throws
        IOException {
      return null;
    }

    @Override
    public FSRecordWriter getRawRecordWriter(Path path, Options options) throws IOException {
      return new MockRecordWriter(path, options);
    }

    @Override
    public FSRecordWriter getHiveRecordWriter(JobConf jc, Path finalOutPath,
                                              Class<? extends Writable> valueClass,
                                              boolean isCompressed, Properties tableProperties,
                                              Progressable progress) throws IOException {
      return null;
    }

    @Override
    public RecordWriter<NullWritable, Text> getRecordWriter(FileSystem fileSystem, JobConf entries,
                                                            String s,
                                                            Progressable progressable) throws
        IOException {
      return null;
    }

    @Override
    public void checkOutputSpecs(FileSystem fileSystem, JobConf entries) throws IOException {

    }
  }

  // This class isn't used and I suspect does totally the wrong thing.  It's only here so that I
  // can provide some output format to the tables and partitions I create.  I actually write to
  // those tables directory.
  static class MockRecordWriter implements FSRecordWriter {
    private FSDataOutputStream os;

    MockRecordWriter(Path basedir, AcidOutputFormat.Options options) throws IOException {
      FileSystem fs = FileSystem.get(options.getConfiguration());
      Path p = AcidUtils.createFilename(basedir, options);
      os = fs.create(p);
    }

    @Override
    public void write(Writable w) throws IOException {
      Text t = (Text)w;
      os.writeBytes(t.toString());
      os.writeBytes("\n");
    }

    @Override
    public void close(boolean abort) throws IOException {
      os.close();
    }
  }


}
