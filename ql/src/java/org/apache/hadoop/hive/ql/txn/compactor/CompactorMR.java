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
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.common.ValidReadTxnList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.ql.io.AcidInputFormat;
import org.apache.hadoop.hive.ql.io.AcidOutputFormat;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.io.RecordIdentifier;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.OutputCommitter;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TaskAttemptContext;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.apache.hadoop.util.StringUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;

/**
 * Class to do compactions via an MR job.  This has to be in the ql package rather than metastore
 * .compactions package with all of it's relatives because it needs access to the actual input
 * and output formats, which are in ql.  ql depends on metastore and we can't have a circular
 * dependency.
 */
public class CompactorMR {

  static final private String CLASS_NAME = CompactorMR.class.getName();
  static final private Log LOG = LogFactory.getLog(CLASS_NAME);

  static final private String INPUT_FORMAT_CLASS_NAME = "hive.compactor.input.format.class.name";
  static final private String OUTPUT_FORMAT_CLASS_NAME = "hive.compactor.output.format.class.name";
  static final private String TMP_LOCATION = "hive.compactor.input.tmp.dir";
  static final private String FINAL_LOCATION = "hive.compactor.input.dir";
  static final private String MIN_TXN = "hive.compactor.txn.min";
  static final private String MAX_TXN = "hive.compactor.txn.max";
  static final private String IS_MAJOR = "hive.compactor.is.major";
  static final private String IS_COMPRESSED = "hive.compactor.is.compressed";
  static final private String TABLE_PROPS = "hive.compactor.table.props";
  static final private String NUM_BUCKETS = "hive.compactor.num.buckets";
  static final private String BASE_DIR = "hive.compactor.base.dir";
  static final private String DELTA_DIRS = "hive.compactor.delta.dirs";
  static final private String DIRS_TO_SEARCH = "hive.compactor.dirs.to.search";
  static final private String TMPDIR = "_tmp";

  public CompactorMR() {
  }

  /**
   * Run a compactor job.
   * @param conf Hive configuration file
   * @param jobName name to run this job with
   * @param t metastore table
   * @param sd metastore storage descriptor
   * @param txns list of valid transactions
   * @param isMajor is this a major compaction?
   * @throws java.io.IOException if the job fails
   */
  void run(HiveConf conf, String jobName, Table t, StorageDescriptor sd,
           ValidTxnList txns, boolean isMajor, Worker.StatsUpdater su) throws IOException {
    JobConf job = new JobConf(conf);
    job.setJobName(jobName);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(NullWritable.class);
    job.setJarByClass(CompactorMR.class);
    LOG.debug("User jar set to " + job.getJar());
    job.setMapperClass(CompactorMap.class);
    job.setNumReduceTasks(0);
    job.setInputFormat(CompactorInputFormat.class);
    job.setOutputFormat(NullOutputFormat.class);
    job.setOutputCommitter(CompactorOutputCommitter.class);

    job.set(FINAL_LOCATION, sd.getLocation());
    job.set(TMP_LOCATION, sd.getLocation() + "/" + TMPDIR + "_" + UUID.randomUUID().toString());
    job.set(INPUT_FORMAT_CLASS_NAME, sd.getInputFormat());
    job.set(OUTPUT_FORMAT_CLASS_NAME, sd.getOutputFormat());
    job.setBoolean(IS_MAJOR, isMajor);
    job.setBoolean(IS_COMPRESSED, sd.isCompressed());
    job.set(TABLE_PROPS, new StringableMap(t.getParameters()).toString());
    job.setInt(NUM_BUCKETS, sd.getNumBuckets());
    job.set(ValidTxnList.VALID_TXNS_KEY, txns.toString());
    setColumnTypes(job, sd.getCols());

    // Figure out and encode what files we need to read.  We do this here (rather than in
    // getSplits below) because as part of this we discover our minimum and maximum transactions,
    // and discovering that in getSplits is too late as we then have no way to pass it to our
    // mapper.

    AcidUtils.Directory dir = AcidUtils.getAcidState(new Path(sd.getLocation()), conf, txns);
    StringableList dirsToSearch = new StringableList();
    Path baseDir = null;
    if (isMajor) {
      // There may not be a base dir if the partition was empty before inserts or if this
      // partition is just now being converted to ACID.
      baseDir = dir.getBaseDirectory();
      if (baseDir == null) {
        List<FileStatus> originalFiles = dir.getOriginalFiles();
        if (!(originalFiles == null) && !(originalFiles.size() == 0)) {
          // There are original format files
          for (FileStatus stat : originalFiles) {
            dirsToSearch.add(stat.getPath());
            LOG.debug("Adding original file " + stat.getPath().toString() + " to dirs to search");
          }
          // Set base to the location so that the input format reads the original files.
          baseDir = new Path(sd.getLocation());
        }
      } else {
        // add our base to the list of directories to search for files in.
        LOG.debug("Adding base directory " + baseDir + " to dirs to search");
        dirsToSearch.add(baseDir);
      }
    }

    List<AcidUtils.ParsedDelta> parsedDeltas = dir.getCurrentDirectories();

    if (parsedDeltas == null || parsedDeltas.size() == 0) {
      // Seriously, no deltas?  Can't compact that.
      LOG.error(  "No delta files found to compact in " + sd.getLocation());
      return;
    }

    StringableList deltaDirs = new StringableList();
    long minTxn = Long.MAX_VALUE;
    long maxTxn = Long.MIN_VALUE;
    for (AcidUtils.ParsedDelta delta : parsedDeltas) {
      LOG.debug("Adding delta " + delta.getPath() + " to directories to search");
      dirsToSearch.add(delta.getPath());
      deltaDirs.add(delta.getPath());
      minTxn = Math.min(minTxn, delta.getMinTransaction());
      maxTxn = Math.max(maxTxn, delta.getMaxTransaction());
    }

    if (baseDir != null) job.set(BASE_DIR, baseDir.toString());
    job.set(DELTA_DIRS, deltaDirs.toString());
    job.set(DIRS_TO_SEARCH, dirsToSearch.toString());
    job.setLong(MIN_TXN, minTxn);
    job.setLong(MAX_TXN, maxTxn);
    LOG.debug("Setting minimum transaction to " + minTxn);
    LOG.debug("Setting maximume transaction to " + maxTxn);

    JobClient.runJob(job).waitForCompletion();
    su.gatherStats();
  }

  /**
   * Set the column names and types into the job conf for the input format
   * to use.
   * @param job the job to update
   * @param cols the columns of the table
   */
  private void setColumnTypes(JobConf job, List<FieldSchema> cols) {
    StringBuilder colNames = new StringBuilder();
    StringBuilder colTypes = new StringBuilder();
    boolean isFirst = true;
    for(FieldSchema col: cols) {
      if (isFirst) {
        isFirst = false;
      } else {
        colNames.append(',');
        colTypes.append(',');
      }
      colNames.append(col.getName());
      colTypes.append(col.getType());
    }
    job.set(serdeConstants.LIST_COLUMNS, colNames.toString());
    job.set(serdeConstants.LIST_COLUMN_TYPES, colTypes.toString());
  }

  static class CompactorInputSplit implements InputSplit {
    private long length = 0;
    private List<String> locations;
    private int bucketNum;
    private Path base;
    private Path[] deltas;

    public CompactorInputSplit() {
    }

    /**
     *
     * @param hadoopConf
     * @param bucket bucket to be processed by this split
     * @param files actual files this split should process.  It is assumed the caller has already
     *              parsed out the files in base and deltas to populate this list.
     * @param base directory of the base, or the partition/table location if the files are in old
     *             style.  Can be null.
     * @param deltas directories of the delta files.
     * @throws IOException
     */
    CompactorInputSplit(Configuration hadoopConf, int bucket, List<Path> files, Path base,
                               Path[] deltas)
        throws IOException {
      bucketNum = bucket;
      this.base = base;
      this.deltas = deltas;
      locations = new ArrayList<String>();

      for (Path path : files) {
        FileSystem fs = path.getFileSystem(hadoopConf);
        FileStatus stat = fs.getFileStatus(path);
        length += stat.getLen();
        BlockLocation[] locs = fs.getFileBlockLocations(stat, 0, length);
        for (int i = 0; i < locs.length; i++) {
          String[] hosts = locs[i].getHosts();
          for (int j = 0; j < hosts.length; j++) {
            locations.add(hosts[j]);
          }
        }
      }
    }

    @Override
    public long getLength() throws IOException {
      return length;
    }

    @Override
    public String[] getLocations() throws IOException {
      return locations.toArray(new String[locations.size()]);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
      dataOutput.writeLong(length);
      dataOutput.writeInt(locations.size());
      for (int i = 0; i < locations.size(); i++) {
        dataOutput.writeInt(locations.get(i).length());
        dataOutput.writeBytes(locations.get(i));
      }
      dataOutput.writeInt(bucketNum);
      if (base == null) {
        dataOutput.writeInt(0);
      } else {
        dataOutput.writeInt(base.toString().length());
        dataOutput.writeBytes(base.toString());
      }
      dataOutput.writeInt(deltas.length);
      for (int i = 0; i < deltas.length; i++) {
        dataOutput.writeInt(deltas[i].toString().length());
        dataOutput.writeBytes(deltas[i].toString());
      }

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
      int len;
      byte[] buf;

      locations = new ArrayList<String>();
      length = dataInput.readLong();
      LOG.debug("Read length of " + length);
      int numElements = dataInput.readInt();
      LOG.debug("Read numElements of " + numElements);
      for (int i = 0; i < numElements; i++) {
        len = dataInput.readInt();
        LOG.debug("Read file length of " + len);
        buf = new byte[len];
        dataInput.readFully(buf);
        locations.add(new String(buf));
      }
      bucketNum = dataInput.readInt();
      LOG.debug("Read bucket number of " + bucketNum);
      len = dataInput.readInt();
      LOG.debug("Read base path length of " + len);
      if (len > 0) {
        buf = new byte[len];
        dataInput.readFully(buf);
        base = new Path(new String(buf));
      }
      numElements = dataInput.readInt();
      deltas = new Path[numElements];
      for (int i = 0; i < numElements; i++) {
        len = dataInput.readInt();
        buf = new byte[len];
        dataInput.readFully(buf);
        deltas[i] = new Path(new String(buf));
      }
    }

    public void set(CompactorInputSplit other) {
      length = other.length;
      locations = other.locations;
      bucketNum = other.bucketNum;
      base = other.base;
      deltas = other.deltas;
    }

    int getBucket() {
      return bucketNum;
    }

    Path getBaseDir() {
      return base;
    }

    Path[] getDeltaDirs() {
      return deltas;
    }

    @Override
    public String toString() {
      StringBuilder builder = new StringBuilder();
      builder.append("CompactorInputSplit{base: ");
      builder.append(base);
      builder.append(", bucket: ");
      builder.append(bucketNum);
      builder.append(", length: ");
      builder.append(length);
      builder.append(", deltas: [");
      for(int i=0; i < deltas.length; ++i) {
        if (i != 0) {
          builder.append(", ");
        }
        builder.append(deltas[i].getName());
      }
      builder.append("]}");
      return builder.toString();
    }
  }

  /**
   * This input format returns its own input split as a value.  This is because our splits
   * contain information needed to properly construct the writer.  Crazy, huh?
   */
  static class CompactorInputFormat implements InputFormat<NullWritable, CompactorInputSplit> {

    @Override
    public InputSplit[] getSplits(JobConf entries, int i) throws IOException {
      Path baseDir = null;
      if (entries.get(BASE_DIR) != null) baseDir = new Path(entries.get(BASE_DIR));
      StringableList tmpDeltaDirs = new StringableList(entries.get(DELTA_DIRS));
      Path[] deltaDirs = tmpDeltaDirs.toArray(new Path[tmpDeltaDirs.size()]);
      StringableList dirsToSearch = new StringableList(entries.get(DIRS_TO_SEARCH));
      Map<Integer, BucketTracker> splitToBucketMap = new HashMap<Integer, BucketTracker>();
      for (Path dir : dirsToSearch) {
        FileSystem fs = dir.getFileSystem(entries);

        // If this is a base or delta directory, then we need to be looking for the bucket files.
        // But if it's a legacy file then we need to add it directly.
        if (dir.getName().startsWith(AcidUtils.BASE_PREFIX) ||
            dir.getName().startsWith(AcidUtils.DELTA_PREFIX)) {
          boolean sawBase = dir.getName().startsWith(AcidUtils.BASE_PREFIX);
          FileStatus[] files = fs.listStatus(dir, AcidUtils.bucketFileFilter);
          for (int j = 0; j < files.length; j++) {
            // For each file, figure out which bucket it is.
            Matcher matcher = AcidUtils.BUCKET_DIGIT_PATTERN.matcher(files[j].getPath().getName());
            addFileToMap(matcher, files[j].getPath(), sawBase, splitToBucketMap);
          }
        } else {
          // Legacy file, see if it's a bucket file
          Matcher matcher = AcidUtils.LEGACY_BUCKET_DIGIT_PATTERN.matcher(dir.getName());
          addFileToMap(matcher, dir, true, splitToBucketMap);
        }
      }
      List<InputSplit> splits = new ArrayList<InputSplit>(splitToBucketMap.size());
      for (Map.Entry<Integer, BucketTracker> e : splitToBucketMap.entrySet()) {
        BucketTracker bt = e.getValue();
        splits.add(new CompactorInputSplit(entries, e.getKey(), bt.buckets,
            bt.sawBase ? baseDir : null, deltaDirs));
      }

      LOG.debug("Returning " + splits.size() + " splits");
      return splits.toArray(new InputSplit[splits.size()]);
    }

    @Override
    public RecordReader<NullWritable, CompactorInputSplit> getRecordReader(
        InputSplit inputSplit,  JobConf entries, Reporter reporter) throws IOException {
      return new CompactorRecordReader((CompactorInputSplit)inputSplit);
    }

    private void addFileToMap(Matcher matcher, Path file, boolean sawBase,
                              Map<Integer, BucketTracker> splitToBucketMap) {
      if (!matcher.find()) {
        LOG.warn("Found a non-bucket file that we thought matched the bucket pattern! " +
            file.toString());
      }
      int bucketNum = Integer.valueOf(matcher.group());
      BucketTracker bt = splitToBucketMap.get(bucketNum);
      if (bt == null) {
        bt = new BucketTracker();
        splitToBucketMap.put(bucketNum, bt);
      }
      LOG.debug("Adding " + file.toString() + " to list of files for splits");
      bt.buckets.add(file);
      bt.sawBase |= sawBase;
    }

    private static class BucketTracker {
      BucketTracker() {
        sawBase = false;
        buckets = new ArrayList<Path>();
      }

      boolean sawBase;
      List<Path> buckets;
    }
  }

  static class CompactorRecordReader
      implements RecordReader<NullWritable, CompactorInputSplit> {
    private CompactorInputSplit split;

    CompactorRecordReader(CompactorInputSplit split) {
      this.split = split;
    }

    @Override
    public boolean next(NullWritable key,
                        CompactorInputSplit compactorInputSplit) throws IOException {
      if (split != null) {
        compactorInputSplit.set(split);
        split = null;
        return true;
      }
      return false;
    }

    @Override
    public NullWritable createKey() {
      return NullWritable.get();
    }

    @Override
    public CompactorInputSplit createValue() {
      return new CompactorInputSplit();
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

  static class CompactorMap<V extends Writable>
      implements Mapper<WritableComparable, CompactorInputSplit,  NullWritable,  NullWritable> {

    JobConf jobConf;
    RecordWriter writer;

    @Override
    public void map(WritableComparable key, CompactorInputSplit split,
                    OutputCollector<NullWritable, NullWritable> nullWritableVOutputCollector,
                    Reporter reporter) throws IOException {
      // This will only get called once, since CompactRecordReader only returns one record,
      // the input split.
      // Based on the split we're passed we go instantiate the real reader and then iterate on it
      // until it finishes.
      @SuppressWarnings("unchecked")//since there is no way to parametrize instance of Class
      AcidInputFormat<WritableComparable, V> aif =
          instantiate(AcidInputFormat.class, jobConf.get(INPUT_FORMAT_CLASS_NAME));
      ValidTxnList txnList =
          new ValidReadTxnList(jobConf.get(ValidTxnList.VALID_TXNS_KEY));

      boolean isMajor = jobConf.getBoolean(IS_MAJOR, false);
      AcidInputFormat.RawReader<V> reader =
          aif.getRawReader(jobConf, isMajor, split.getBucket(),
              txnList, split.getBaseDir(), split.getDeltaDirs());
      RecordIdentifier identifier = reader.createKey();
      V value = reader.createValue();
      getWriter(reporter, reader.getObjectInspector(), split.getBucket());
      while (reader.next(identifier, value)) {
        if (isMajor && reader.isDelete(value)) continue;
        writer.write(value);
        reporter.progress();
      }
    }

    @Override
    public void configure(JobConf entries) {
      jobConf = entries;
    }

    @Override
    public void close() throws IOException {
      if (writer != null) {
        writer.close(false);
      }
    }

    private void getWriter(Reporter reporter, ObjectInspector inspector,
                           int bucket) throws IOException {
      if (writer == null) {
        AcidOutputFormat.Options options = new AcidOutputFormat.Options(jobConf);
        options.inspector(inspector)
            .writingBase(jobConf.getBoolean(IS_MAJOR, false))
            .isCompressed(jobConf.getBoolean(IS_COMPRESSED, false))
            .tableProperties(new StringableMap(jobConf.get(TABLE_PROPS)).toProperties())
            .reporter(reporter)
            .minimumTransactionId(jobConf.getLong(MIN_TXN, Long.MAX_VALUE))
            .maximumTransactionId(jobConf.getLong(MAX_TXN, Long.MIN_VALUE))
            .bucket(bucket);

        // Instantiate the underlying output format
        @SuppressWarnings("unchecked")//since there is no way to parametrize instance of Class
        AcidOutputFormat<WritableComparable, V> aof =
            instantiate(AcidOutputFormat.class, jobConf.get(OUTPUT_FORMAT_CLASS_NAME));

        writer = aof.getRawRecordWriter(new Path(jobConf.get(TMP_LOCATION)), options);
      }
    }

  }

  static class StringableMap extends HashMap<String, String> {

    StringableMap(String s) {
      String[] parts = s.split(":", 2);
      // read that many chars
      int numElements = Integer.valueOf(parts[0]);
      s = parts[1];
      for (int i = 0; i < numElements; i++) {
        parts = s.split(":", 2);
        int len = Integer.valueOf(parts[0]);
        String key = null;
        if (len > 0) key = parts[1].substring(0, len);
        parts = parts[1].substring(len).split(":", 2);
        len = Integer.valueOf(parts[0]);
        String value = null;
        if (len > 0) value = parts[1].substring(0, len);
        s = parts[1].substring(len);
        put(key, value);
      }
    }

    StringableMap(Map<String, String> m) {
      super(m);
    }

    @Override
    public String toString() {
      StringBuffer buf = new StringBuffer();
      buf.append(size());
      buf.append(':');
      if (size() > 0) {
        for (Map.Entry<String, String> entry : entrySet()) {
          int length = (entry.getKey() == null) ? 0 : entry.getKey().length();
          buf.append(entry.getKey() == null ? 0 : length);
          buf.append(':');
          if (length > 0) buf.append(entry.getKey());
          length = (entry.getValue() == null) ? 0 : entry.getValue().length();
          buf.append(length);
          buf.append(':');
          if (length > 0) buf.append(entry.getValue());
        }
      }
      return buf.toString();
    }

    public Properties toProperties() {
      Properties props = new Properties();
      props.putAll(this);
      return props;
    }
  }

  static class StringableList extends ArrayList<Path> {
    StringableList() {

    }

    StringableList(String s) {
      String[] parts = s.split(":", 2);
      // read that many chars
      int numElements = Integer.valueOf(parts[0]);
      s = parts[1];
      for (int i = 0; i < numElements; i++) {
        parts = s.split(":", 2);
        int len = Integer.valueOf(parts[0]);
        String val = parts[1].substring(0, len);
        s = parts[1].substring(len);
        add(new Path(val));
      }
    }

    @Override
    public String toString() {
      StringBuffer buf = new StringBuffer();
      buf.append(size());
      buf.append(':');
      if (size() > 0) {
        for (Path p : this) {
          buf.append(p.toString().length());
          buf.append(':');
          buf.append(p.toString());
        }
      }
      return buf.toString();
    }
  }

  private static <T> T instantiate(Class<T> classType, String classname) throws IOException {
    T t = null;
    try {
      Class c = JavaUtils.loadClass(classname);
      Object o = c.newInstance();
      if (classType.isAssignableFrom(o.getClass())) {
        t = (T)o;
      } else {
        String s = classname + " is not an instance of " + classType.getName();
        LOG.error(s);
        throw new IOException(s);
      }
    } catch (ClassNotFoundException e) {
      LOG.error("Unable to instantiate class, " + StringUtils.stringifyException(e));
      throw new IOException(e);
    } catch (InstantiationException e) {
      LOG.error("Unable to instantiate class, " + StringUtils.stringifyException(e));
      throw new IOException(e);
    } catch (IllegalAccessException e) {
      LOG.error("Unable to instantiate class, " + StringUtils.stringifyException(e));
      throw new IOException(e);
    }
    return t;
  }

  static class CompactorOutputCommitter extends OutputCommitter {

    @Override
    public void setupJob(JobContext jobContext) throws IOException {

    }

    @Override
    public void setupTask(TaskAttemptContext taskAttemptContext) throws IOException {

    }

    @Override
    public boolean needsTaskCommit(TaskAttemptContext taskAttemptContext) throws IOException {
      return false;
    }

    @Override
    public void commitTask(TaskAttemptContext taskAttemptContext) throws IOException {

    }

    @Override
    public void abortTask(TaskAttemptContext taskAttemptContext) throws IOException {

    }

    @Override
    public void commitJob(JobContext context) throws IOException {
      JobConf conf = ShimLoader.getHadoopShims().getJobConf(context);
      Path tmpLocation = new Path(conf.get(TMP_LOCATION));
      Path finalLocation = new Path(conf.get(FINAL_LOCATION));
      FileSystem fs = tmpLocation.getFileSystem(conf);
      LOG.debug("Moving contents of " + tmpLocation.toString() + " to " +
          finalLocation.toString());

      FileStatus[] contents = fs.listStatus(tmpLocation);
      for (int i = 0; i < contents.length; i++) {
        Path newPath = new Path(finalLocation, contents[i].getPath().getName());
        fs.rename(contents[i].getPath(), newPath);
      }
      fs.delete(tmpLocation, true);
    }

    @Override
    public void abortJob(JobContext context, int status) throws IOException {
      JobConf conf = ShimLoader.getHadoopShims().getJobConf(context);
      Path tmpLocation = new Path(conf.get(TMP_LOCATION));
      FileSystem fs = tmpLocation.getFileSystem(conf);
      LOG.debug("Removing " + tmpLocation.toString());
      fs.delete(tmpLocation, true);
    }
  }
}
