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
package org.apache.hadoop.hive.ql.txn.compactor;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Matcher;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.common.ValidCompactorWriteIdList;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.CompactionType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.metastore.txn.entities.CompactionInfo;
import org.apache.hadoop.hive.metastore.utils.StringableMap;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.ql.io.AcidInputFormat;
import org.apache.hadoop.hive.ql.io.AcidOutputFormat;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.io.AcidDirectory;
import org.apache.hadoop.hive.ql.io.HiveInputFormat;
import org.apache.hadoop.hive.ql.io.IOConstants;
import org.apache.hadoop.hive.ql.io.RecordIdentifier;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.shims.HadoopShims.HdfsFileStatusWithId;
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
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TaskAttemptContext;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hive.common.util.Ref;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class to do compactions via an MR job.  This has to be in the ql package rather than metastore
 * .compactions package with all of it's relatives because it needs access to the actual input
 * and output formats, which are in ql.  ql depends on metastore and we can't have a circular
 * dependency.
 */
public class MRCompactor implements Compactor {

  private static final String CLASS_NAME = MRCompactor.class.getName();
  private static final Logger LOG = LoggerFactory.getLogger(CLASS_NAME);
  private static final String INPUT_FORMAT_CLASS_NAME = "hive.compactor.input.format.class.name";
  private static final String OUTPUT_FORMAT_CLASS_NAME = "hive.compactor.output.format.class.name";
  private static final String TMP_LOCATION = "hive.compactor.input.tmp.dir";
  private static final String MIN_TXN = "hive.compactor.txn.min";
  private static final String MAX_TXN = "hive.compactor.txn.max";
  private static final String IS_MAJOR = "hive.compactor.is.major";
  private static final String IS_COMPRESSED = "hive.compactor.is.compressed";
  private static final String TABLE_PROPS = "hive.compactor.table.props";
  private static final String NUM_BUCKETS = hive_metastoreConstants.BUCKET_COUNT;
  private static final String BASE_DIR = "hive.compactor.base.dir";
  private static final String DELTA_DIRS = "hive.compactor.delta.dirs";
  private static final String DIRS_TO_SEARCH = "hive.compactor.dirs.to.search";
  private static final String TMPDIR = "_tmp";
  private static final String TBLPROPS_PREFIX = "tblprops.";
  private static final String COMPACTOR_PREFIX = "compactor.";

  private JobConf mrJob;  // the MR job for compaction
  private IMetaStoreClient msc;
  public MRCompactor(IMetaStoreClient msc) {
    this.msc = msc;
  }

  @Override
  public boolean run(CompactorContext context)
       throws IOException, HiveException, InterruptedException {
    if (context.getCompactionInfo().runAs.equals(System.getProperty("user.name"))) {
      run(context.getConf(), context.getTable(), context.getSd(),
              context.getValidWriteIdList(), context.getCompactionInfo(), context.getAcidDirectory());
    } else {
      UserGroupInformation ugi = UserGroupInformation.createProxyUser(context.getCompactionInfo().runAs,
              UserGroupInformation.getLoginUser());
      ugi.doAs((PrivilegedExceptionAction<Object>) () -> {
        run(context.getConf(), context.getTable(), context.getSd(),
                context.getValidWriteIdList(), context.getCompactionInfo(), context.getAcidDirectory());
        return null;
      });
      try {
        FileSystem.closeAllForUGI(ugi);
      } catch (IOException exception) {
        LOG.error("Could not clean up file-system handles for UGI: " + ugi + " for " + context.getCompactionInfo().getFullPartitionName(),
            exception);
      }
    }
    return true;
  }

  @VisibleForTesting
  public JobConf createBaseJobConf(HiveConf conf, String jobName, Table t, StorageDescriptor sd,
                                    ValidWriteIdList writeIds, CompactionInfo ci) {
    JobConf job = new JobConf(conf);
    job.setJobName(jobName);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(NullWritable.class);
    job.setJarByClass(MRCompactor.class);
    if (LOG.isDebugEnabled()) {
      LOG.debug("User jar set to {}", job.getJar());
    }
    job.setMapperClass(CompactorMap.class);
    job.setNumReduceTasks(0);
    job.setInputFormat(CompactorInputFormat.class);
    job.setOutputFormat(NullOutputFormat.class);
    job.setOutputCommitter(CompactorOutputCommitter.class);

    job.set(FINAL_LOCATION, sd.getLocation());
    job.set(TMP_LOCATION, generateTmpPath(sd));
    job.set(INPUT_FORMAT_CLASS_NAME, sd.getInputFormat());
    job.set(OUTPUT_FORMAT_CLASS_NAME, sd.getOutputFormat());
    job.setBoolean(IS_COMPRESSED, sd.isCompressed());
    job.set(TABLE_PROPS, new StringableMap(t.getParameters()).toString());
    job.setInt(NUM_BUCKETS, sd.getNumBuckets());
    job.set(ValidWriteIdList.VALID_WRITEIDS_KEY, writeIds.toString());
    overrideMRProps(job, t.getParameters()); // override MR properties from tblproperties if applicable
    if (ci.properties != null) {
      overrideTblProps(job, t.getParameters(), ci.properties);
    }

    String queueName = CompactorUtil.getCompactorJobQueueName(conf, ci, t);
    if (!queueName.isEmpty()) {
      job.setQueueName(queueName);
    }

    // have to use table columns since partition SD isn't updated if these are altered
    setColumnTypes(job, t.getSd().getCols());
    //with feature on, multiple tasks may get into conflict creating/using TMP_LOCATION and if we were
    //to generate the target dir in the Map task, there is no easy way to pass it to OutputCommitter
    //to do the final move
    job.setBoolean("mapreduce.map.speculative", false);

    // Set appropriate Acid readers/writers based on the table properties.
    AcidUtils.setAcidOperationalProperties(job, true,
            AcidUtils.getAcidOperationalProperties(t.getParameters()));

    return job;
  }

  /**
   * Parse tblproperties specified on "ALTER TABLE ... COMPACT ... WITH OVERWRITE TBLPROPERTIES ..."
   * and override two categories of properties:
   * 1. properties of the compactor MR job (with prefix "compactor.")
   * 2. general hive properties (with prefix "tblprops.")
   * @param job the compactor MR job
   * @param tblproperties existing tblproperties
   * @param properties table properties
   */
  private void overrideTblProps(JobConf job, Map<String, String> tblproperties, String properties) {
    StringableMap stringableMap = new StringableMap(properties);
    overrideMRProps(job, stringableMap);
    // mingle existing tblproperties with those specified on the ALTER TABLE command
    for (String key : stringableMap.keySet()) {
      if (key.startsWith(TBLPROPS_PREFIX)) {
        String propKey = key.substring(9);  // 9 is the length of "tblprops.". We only keep the rest
        tblproperties.put(propKey, stringableMap.get(key));
      }
    }
    // re-set TABLE_PROPS with reloaded tblproperties
    job.set(TABLE_PROPS, new StringableMap(tblproperties).toString());
  }

  /**
   * Parse tblproperties to override relevant properties of compactor MR job with specified values.
   * For example, compactor.mapreuce.map.memory.mb=1024
   * @param job the compactor MR job
   * @param properties table properties
   */
  private void overrideMRProps(JobConf job, Map<String, String> properties) {
    for (String key : properties.keySet()) {
      if (key.startsWith(COMPACTOR_PREFIX)) {
        String mrKey = key.substring(10); // 10 is the length of "compactor." We only keep the rest.
        job.set(mrKey, properties.get(key));
      }
    }
  }

  /**
   * Run Compaction which may consist of several jobs on the cluster.
   * @param conf Hive configuration file
   * @param t metastore table
   * @param sd metastore storage descriptor
   * @param writeIds list of valid write ids
   * @param ci CompactionInfo
   * @throws java.io.IOException if the job fails
   */
  public void run(HiveConf conf, Table t, StorageDescriptor sd, ValidWriteIdList writeIds,
           CompactionInfo ci, AcidDirectory dir) throws IOException {

    String jobName = ci.workerId + "-compactor-" + ci.getFullPartitionName();
    JobConf job = createBaseJobConf(conf, jobName, t, sd, writeIds, ci);

    List<AcidUtils.ParsedDelta> parsedDeltas = dir.getCurrentDirectories();
    int maxDeltasToHandle = conf.getIntVar(HiveConf.ConfVars.COMPACTOR_MAX_NUM_DELTA);
    if (parsedDeltas.size() > maxDeltasToHandle) {
      /*
       * if here, that means we have very high number of delta files.  This may be sign of a temporary
       * glitch or a real issue.  For example, if transaction batch size or transaction size is set too
       * low for the event flow rate in Streaming API, it may generate lots of delta files very
       * quickly.  Another possibility is that Compaction is repeatedly failing and not actually compacting.
       * Thus, force N minor compactions first to reduce number of deltas and then follow up with
       * the compaction actually requested in {@link ci} which now needs to compact a lot fewer deltas
       */
      if (LOG.isWarnEnabled()) {
        LOG.warn("{} delta files found for {} located at {}! This is likely a sign of misconfiguration, " +
                "especially if this message repeats.  Check that compaction is running properly.  Check for any " +
                "runaway/mis-configured process writing to ACID tables, especially using Streaming Ingest API.",
            parsedDeltas.size(), ci.getFullPartitionName(), sd.getLocation());
      }
      int numMinorCompactions = parsedDeltas.size() / maxDeltasToHandle;
      parsedDeltas.sort(AcidUtils.ParsedDeltaLight::compareTo);

      int start = 0;
      int end = maxDeltasToHandle;

      for (int jobSubId = 0; jobSubId < numMinorCompactions; jobSubId++) {
        while (end > 0 && end < parsedDeltas.size() &&
          parsedDeltas.get(end).getMinWriteId() == parsedDeltas.get(end - 1).getMinWriteId() &&
          parsedDeltas.get(end).getMaxWriteId() == parsedDeltas.get(end - 1).getMaxWriteId()) {
          end--;
        }
        List<AcidUtils.ParsedDelta> split = parsedDeltas.subList(start, end);
        start = end;
        end = start + maxDeltasToHandle;

        JobConf jobMinorCompact = createBaseJobConf(conf, jobName + "_" + jobSubId, t, sd, writeIds, ci);
        launchCompactionJob(jobMinorCompact,
          null, CompactionType.MINOR, null,
            split, split.size(), -1, conf, msc, ci.id, jobName);
      }
      //now recompute state since we've done minor compactions and have different 'best' set of deltas
      dir = AcidUtils.getAcidState(null, new Path(sd.getLocation()), conf, writeIds, Ref.from(false), false);
    }

    StringableList dirsToSearch = new StringableList();
    Path baseDir = null;
    if (CompactionType.MAJOR.equals(ci.type)) {
      // There may not be a base dir if the partition was empty before inserts or if this
      // partition is just now being converted to ACID.
      baseDir = dir.getBaseDirectory();
      if (baseDir == null) {
        List<HdfsFileStatusWithId> originalFiles = dir.getOriginalFiles();
        if (originalFiles != null && !originalFiles.isEmpty()) {
          // There are original format files
          for (HdfsFileStatusWithId stat : originalFiles) {
            Path path = stat.getFileStatus().getPath();
            //note that originalFiles are all original files recursively not dirs
            dirsToSearch.add(path);
            if (LOG.isDebugEnabled()) {
              LOG.debug("Adding original file {} to dirs to search", path);
            }
          }
          // Set base to the location so that the input format reads the original files.
          baseDir = new Path(sd.getLocation());
        }
      } else {
        // add our base to the list of directories to search for files in.
        if (LOG.isDebugEnabled()) {
          LOG.debug("Adding base directory {} to dirs to search", baseDir);
        }
        dirsToSearch.add(baseDir);
      }
    }

    launchCompactionJob(job, baseDir, ci.type, dirsToSearch, dir.getCurrentDirectories(),
      dir.getCurrentDirectories().size(), dir.getObsolete().size(), conf, msc, ci.id, jobName);
  }

  /**
   * Generate a random tmp path, under the provided storage.
   * @param sd storage descriptor, must be not null.
   * @return path, always not null
   */
  private static String generateTmpPath(StorageDescriptor sd) {
    return sd.getLocation() + "/" + TMPDIR + "_" + UUID.randomUUID().toString();
  }

  /**
   * @param baseDir if not null, it's either table/partition root folder or base_xxxx.
   *                If it's base_xxxx, it's in dirsToSearch, else the actual original files
   *                (all leaves recursively) are in the dirsToSearch list
   */
  private void launchCompactionJob(JobConf job, Path baseDir, CompactionType compactionType,
                                   StringableList dirsToSearch,
                                   List<AcidUtils.ParsedDelta> parsedDeltas,
                                   int curDirNumber, int obsoleteDirNumber, HiveConf hiveConf,
                                   IMetaStoreClient msc, long id, String jobName) throws IOException {
    job.setBoolean(IS_MAJOR, compactionType == CompactionType.MAJOR);
    if(dirsToSearch == null) {
      dirsToSearch = new StringableList();
    }
    StringableList deltaDirs = new StringableList();
    // Note: if compaction creates a delta, it won't replace an existing base dir, so the txn ID
    //       of the base dir won't be a part of delta's range. If otoh compaction creates a base,
    //       we don't care about this value because bases don't have min txn ID in the name.
    //       However logically this should also take base into account if it's included.
    long minTxn = Long.MAX_VALUE;
    long maxTxn = Long.MIN_VALUE;
    for (AcidUtils.ParsedDelta delta : parsedDeltas) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Adding delta {} to directories to search", delta.getPath());
      }
      dirsToSearch.add(delta.getPath());
      deltaDirs.add(delta.getPath());
      minTxn = Math.min(minTxn, delta.getMinWriteId());
      maxTxn = Math.max(maxTxn, delta.getMaxWriteId());
    }

    if (baseDir != null) job.set(BASE_DIR, baseDir.toString());
    job.set(DELTA_DIRS, deltaDirs.toString());
    job.set(DIRS_TO_SEARCH, dirsToSearch.toString());
    job.setLong(MIN_TXN, minTxn);
    job.setLong(MAX_TXN, maxTxn);
    // HIVE-23354 enforces that MR speculative execution is disabled
    job.setBoolean(MRJobConfig.REDUCE_SPECULATIVE, false);
    job.setBoolean(MRJobConfig.MAP_SPECULATIVE, false);

    // Add tokens for all the file system in the input path.
    ArrayList<Path> dirs = new ArrayList<>();
    if (baseDir != null) {
      dirs.add(baseDir);
    }
    dirs.addAll(deltaDirs);
    dirs.addAll(dirsToSearch);
    TokenCache.obtainTokensForNamenodes(job.getCredentials(), dirs.toArray(new Path[]{}), job);

    if (hiveConf.getBoolVar(HiveConf.ConfVars.HIVE_IN_TEST)) {
      mrJob = job;
    }
    if (LOG.isInfoEnabled()) {
      LOG.info("Submitting {} compaction job '{}' to {} queue. (current delta dirs count={}, obsolete delta dirs " +
              "count={}. TxnIdRange[{},{}}]",
          compactionType, job.getJobName(), job.getQueueName(), curDirNumber, obsoleteDirNumber, minTxn, maxTxn);
    }
    try (JobClient jc = new JobClient(job)) {
      RunningJob rj = jc.submitJob(job);
      if (LOG.isInfoEnabled()) {
        LOG.info("Submitted compaction job '{}' with jobID={} compaction ID={}", job.getJobName(), rj.getID(), id);
      }
      try {
        msc.setHadoopJobid(rj.getID().toString(), id);
      } catch (TException e) {
        LOG.warn("Error setting hadoop job, jobId=" + rj.getID().toString()
            + " compactionId=" + id, e);
      }
      rj.waitForCompletion();
      if (!rj.isSuccessful()) {
        throw new IOException((compactionType == CompactionType.MAJOR ? "Major" : "Minor") +
            " compactor job failed for " + jobName + "! Hadoop JobId: " + rj.getID());
      }
    }
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
    job.set(IOConstants.SCHEMA_EVOLUTION_COLUMNS, colNames.toString());
    job.set(IOConstants.SCHEMA_EVOLUTION_COLUMNS_TYPES, colTypes.toString());
    HiveConf.setVar(job, HiveConf.ConfVars.HIVE_INPUT_FORMAT, HiveInputFormat.class.getName());
  }

  public JobConf getMrJob() {
    return mrJob;
  }

  static class CompactorInputSplit implements InputSplit {
    private long length = 0;
    private List<String> locations;
    private int bucketNum;
    private Path base;
    private Path[] deltas;
    private Map<String, Integer> deltasToAttemptId;

    public CompactorInputSplit() {
    }

    /**
     *
     * @param hadoopConf
     * @param bucket bucket to be processed by this split
     * @param files actual files this split should process.  It is assumed the caller has already
     *              parsed out the files in base and deltas to populate this list.  Includes copy_N
     * @param base directory of the base, or the partition/table location if the files are in old
     *             style.  Can be null.
     * @param deltas directories of the delta files.
     * @throws IOException
     */
    CompactorInputSplit(Configuration hadoopConf, int bucket, List<Path> files, Path base,
                               Path[] deltas, Map<String, Integer> deltasToAttemptId)
        throws IOException {
      bucketNum = bucket;
      this.base = base;
      this.deltas = deltas;
      locations = new ArrayList<>();
      this.deltasToAttemptId = deltasToAttemptId;

      for (Path path : files) {
        FileSystem fs = path.getFileSystem(hadoopConf);
        FileStatus stat = fs.getFileStatus(path);
        length += stat.getLen();
        for (BlockLocation loc : fs.getFileBlockLocations(stat, 0, length)) {
          Collections.addAll(locations, loc.getHosts());
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
        Integer attemptId = deltasToAttemptId.get(base.getName());
        if (attemptId == null) {
          dataOutput.writeInt(0);
        } else {
          dataOutput.writeInt(attemptId.toString().length());
          dataOutput.writeBytes(attemptId.toString());
        }
      }
      dataOutput.writeInt(deltas.length);
      for (int i = 0; i < deltas.length; i++) {
        dataOutput.writeInt(deltas[i].toString().length());
        dataOutput.writeBytes(deltas[i].toString());
        Integer attemptId = deltasToAttemptId.get(deltas[i].getName());
        if (attemptId == null) {
          dataOutput.writeInt(0);
        } else {
          dataOutput.writeInt(attemptId.toString().length());
          dataOutput.writeBytes(attemptId.toString());
        }
      }

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
      int len;
      byte[] buf;

      locations = new ArrayList<>();
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
      Integer baseAttemptId = null;
      if (len > 0) {
        buf = new byte[len];
        dataInput.readFully(buf);
        base = new Path(new String(buf));
        len = dataInput.readInt();
        if (len > 0) {
          buf = new byte[len];
          dataInput.readFully(buf);
          String baseAttemptIdString = new String(buf);
          baseAttemptId = Integer.valueOf(baseAttemptIdString);
        }
      }
      numElements = dataInput.readInt();
      deltasToAttemptId = new HashMap<>();
      deltas = new Path[numElements];
      for (int i = 0; i < numElements; i++) {
        len = dataInput.readInt();
        buf = new byte[len];
        dataInput.readFully(buf);
        deltas[i] = new Path(new String(buf));
        len = dataInput.readInt();
        Integer attemptId = null;
        if (len > 0) {
          buf = new byte[len];
          dataInput.readFully(buf);
          String attemptIdString = new String(buf);
          attemptId = Integer.valueOf(attemptIdString);
        }
        deltasToAttemptId.put(deltas[i].getName(), attemptId);
        if (baseAttemptId != null) {
          deltasToAttemptId.put(base.getName(), baseAttemptId);
        }
      }
      if (baseAttemptId != null) {
        deltasToAttemptId.put(base.toString(), baseAttemptId);
      }
    }

    public void set(CompactorInputSplit other) {
      length = other.length;
      locations = other.locations;
      bucketNum = other.bucketNum;
      base = other.base;
      deltas = other.deltas;
      deltasToAttemptId = other.deltasToAttemptId;
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

    Map<String, Integer> getDeltasToAttemptId() {
      return deltasToAttemptId;
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
        // When we have split-update and there are two kinds of delta directories-
        // the delta_x_y/ directory one which has only insert events and
        // the delete_delta_x_y/ directory which has only the delete events.
        // The clever thing about this kind of splitting is that everything in the delta_x_y/
        // directory can be processed as base files. However, this is left out currently
        // as an improvement for the future.

        if (dir.getName().startsWith(AcidUtils.BASE_PREFIX) ||
            dir.getName().startsWith(AcidUtils.DELTA_PREFIX) ||
            dir.getName().startsWith(AcidUtils.DELETE_DELTA_PREFIX)) {
          boolean sawBase = dir.getName().startsWith(AcidUtils.BASE_PREFIX);
          boolean isRawFormat = !dir.getName().startsWith(AcidUtils.DELETE_DELTA_PREFIX)
            && AcidUtils.MetaDataFile.isRawFormat(dir, fs, null);//deltes can't be raw format

          FileStatus[] files = fs.listStatus(dir, isRawFormat ? AcidUtils.originalBucketFilter
            : AcidUtils.bucketFileFilter);
          for(FileStatus f : files) {
            // For each file, figure out which bucket it is.
            Matcher matcher = isRawFormat ?
              AcidUtils.LEGACY_BUCKET_DIGIT_PATTERN.matcher(f.getPath().getName())
              : AcidUtils.BUCKET_PATTERN.matcher(f.getPath().getName());
            addFileToMap(matcher, f.getPath(), sawBase, splitToBucketMap);
          }
        } else {
          // Legacy file, see if it's a bucket file
          Matcher matcher = AcidUtils.LEGACY_BUCKET_DIGIT_PATTERN.matcher(dir.getName());
          addFileToMap(matcher, dir, true, splitToBucketMap);
        }
      }

      boolean isTableBucketed = entries.getInt(NUM_BUCKETS, -1) != -1;

      List<InputSplit> splits = new ArrayList<InputSplit>(splitToBucketMap.size());
      for (Map.Entry<Integer, BucketTracker> e : splitToBucketMap.entrySet()) {
        BucketTracker bt = e.getValue();
        // For non-bucketed tables we might not have a 00000x_0 in all the delta dirs e.g. after
        // multiple ingestions of various sizes.
        Path[] deltasForSplit = isTableBucketed ? deltaDirs : getDeltaDirsFromBucketTracker(bt);
        splits.add(new CompactorInputSplit(entries, e.getKey(), bt.buckets,
            bt.sawBase ? baseDir : null, deltasForSplit, bt.deltasToAttemptId));
      }

      LOG.debug("Returning " + splits.size() + " splits");
      return splits.toArray(new InputSplit[splits.size()]);
    }

    private static Path[] getDeltaDirsFromBucketTracker(BucketTracker bucketTracker) {
      List<Path> resultList = new ArrayList<>(bucketTracker.buckets.size());

      for (int i = 0; i < bucketTracker.buckets.size(); ++i) {
        Path p = bucketTracker.buckets.get(i).getParent();
        if (p.getName().startsWith(AcidUtils.DELTA_PREFIX) ||
            p.getName().startsWith(AcidUtils.DELETE_DELTA_PREFIX)) {
          resultList.add(p);
        }
      }

      return resultList.toArray(new Path[0]);
    }

    @Override
    public RecordReader<NullWritable, CompactorInputSplit> getRecordReader(
        InputSplit inputSplit,  JobConf entries, Reporter reporter) throws IOException {
      return new CompactorRecordReader((CompactorInputSplit)inputSplit);
    }

    private void addFileToMap(Matcher matcher, Path file, boolean sawBase,
                              Map<Integer, BucketTracker> splitToBucketMap) {
      if (!matcher.find()) {
        String msg = "Found a non-bucket file that we thought matched the bucket pattern! " +
          file.toString() + " Matcher=" + matcher.toString();
        LOG.error(msg);
        //following matcher.group() would fail anyway and we don't want to skip files since that
        //may be a data loss scenario
        throw new IllegalArgumentException(msg);
      }
      int bucketNum = -1;
      Integer attemptId = null;
      if (matcher.groupCount() > 0) {
        bucketNum = Integer.parseInt(matcher.group(1));
        attemptId = matcher.group(2) != null ? Integer.valueOf(matcher.group(2).substring(1)) : null;
      } else {
        bucketNum = Integer.valueOf(matcher.group());
      }

      BucketTracker bt = splitToBucketMap.get(bucketNum);
      if (bt == null) {
        bt = new BucketTracker();
        splitToBucketMap.put(bucketNum, bt);
      }
      LOG.debug("Adding " + file.toString() + " to list of files for splits");
      bt.buckets.add(file);
      bt.sawBase |= sawBase;
      bt.deltasToAttemptId.put(file.getParent().getName(), attemptId);
    }

    private static class BucketTracker {
      BucketTracker() {
        sawBase = false;
        buckets = new ArrayList<Path>();
        deltasToAttemptId = new HashMap<>();
      }

      boolean sawBase;
      List<Path> buckets;
      Map<String, Integer> deltasToAttemptId;
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

  @VisibleForTesting
  public static class CompactorMap<V extends Writable>
      implements Mapper<WritableComparable, CompactorInputSplit,  NullWritable,  NullWritable> {

    JobConf jobConf;
    RecordWriter writer = null;
    RecordWriter deleteEventWriter = null;

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
      ValidWriteIdList writeIdList =
          new ValidCompactorWriteIdList(jobConf.get(ValidWriteIdList.VALID_WRITEIDS_KEY));

      boolean isMajor = jobConf.getBoolean(IS_MAJOR, false);
      AcidInputFormat.RawReader<V> reader =
          aif.getRawReader(jobConf, isMajor, split.getBucket(),
                  writeIdList, split.getBaseDir(), split.getDeltaDirs(), split.getDeltasToAttemptId());
      RecordIdentifier identifier = reader.createKey();
      V value = reader.createValue();

      AcidUtils.AcidOperationalProperties acidOperationalProperties = AcidUtils.getAcidOperationalProperties(jobConf);

      while (reader.next(identifier, value)) {
        boolean sawDeleteRecord = reader.isDelete(value);
        if (isMajor && sawDeleteRecord) {
          continue;
        }
        if (sawDeleteRecord && acidOperationalProperties.isSplitUpdate()) {
          getDeleteEventWriter(reporter, reader.getObjectInspector(), split.getBucket())
                  .write(value);
          reporter.progress();
        } else {
          getWriter(reporter, reader.getObjectInspector(), split.getBucket())
                  .write(value);
          reporter.progress();
        }
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
      if (deleteEventWriter != null) {
        deleteEventWriter.close(false);
      }
    }

    private RecordWriter getWriter(Reporter reporter, ObjectInspector inspector,
                           int bucket) throws IOException {
      if (writer == null) {
        AcidOutputFormat.Options options = new AcidOutputFormat.Options(jobConf);
        options.inspector(inspector)
            .writingBase(jobConf.getBoolean(IS_MAJOR, false))
            .isCompressed(jobConf.getBoolean(IS_COMPRESSED, false))
            .tableProperties(new StringableMap(jobConf.get(TABLE_PROPS)).toProperties())
            .reporter(reporter)
            .minimumWriteId(jobConf.getLong(MIN_TXN, Long.MAX_VALUE))
            .maximumWriteId(jobConf.getLong(MAX_TXN, Long.MIN_VALUE))
            .bucket(bucket)
            .statementId(-1)//setting statementId == -1 makes compacted delta files use
            .visibilityTxnId(Compactor.getCompactorTxnId(jobConf));
      //delta_xxxx_yyyy format

        // Instantiate the underlying output format
        @SuppressWarnings("unchecked")//since there is no way to parametrize instance of Class
        AcidOutputFormat<WritableComparable, V> aof =
            instantiate(AcidOutputFormat.class, jobConf.get(OUTPUT_FORMAT_CLASS_NAME));

        Path rootDir = new Path(jobConf.get(TMP_LOCATION));
        cleanupTmpLocationOnTaskRetry(options, rootDir);
        writer = aof.getRawRecordWriter(rootDir, options);
      }
      return writer;
    }

    @VisibleForTesting
    public void cleanupTmpLocationOnTaskRetry(AcidOutputFormat.Options options, Path rootDir) throws IOException {
      Path tmpLocation = AcidUtils.createFilename(rootDir, options);
      FileSystem fs = tmpLocation.getFileSystem(jobConf);

      try {
        fs.delete(tmpLocation, true);
      } catch (FileNotFoundException e) {
        // no problem
      }
    }

    private RecordWriter getDeleteEventWriter(Reporter reporter, ObjectInspector inspector,
        int bucket) throws IOException {
      if (deleteEventWriter == null) {
        AcidOutputFormat.Options options = new AcidOutputFormat.Options(jobConf);
        options.inspector(inspector).writingBase(false)
                .writingDeleteDelta(true)   // this is the option which will make it a delete writer
                .isCompressed(jobConf.getBoolean(IS_COMPRESSED, false))
                .tableProperties(new StringableMap(jobConf.get(TABLE_PROPS)).toProperties()).reporter(reporter)
                .minimumWriteId(jobConf.getLong(MIN_TXN, Long.MAX_VALUE))
                .maximumWriteId(jobConf.getLong(MAX_TXN, Long.MIN_VALUE)).bucket(bucket)
                .statementId(-1)//setting statementId == -1 makes compacted delta files use
                // delta_xxxx_yyyy format
                .visibilityTxnId(Compactor.getCompactorTxnId(jobConf));

        // Instantiate the underlying output format
        @SuppressWarnings("unchecked")//since there is no way to parametrize instance of Class
        AcidOutputFormat<WritableComparable, V> aof =
                instantiate(AcidOutputFormat.class, jobConf.get(OUTPUT_FORMAT_CLASS_NAME));

        Path rootDir = new Path(jobConf.get(TMP_LOCATION));
        cleanupTmpLocationOnTaskRetry(options, rootDir);
        deleteEventWriter = aof.getRawRecordWriter(rootDir, options);
      }
      return deleteEventWriter;
    }
  }

  static class StringableList extends ArrayList<Path> {
    StringableList() {

    }

    StringableList(String s) {
      String[] parts = s.split(":", 2);
      // read that many chars
      int numElements = Integer.parseInt(parts[0]);
      s = parts[1];
      for (int i = 0; i < numElements; i++) {
        parts = s.split(":", 2);
        int len = Integer.parseInt(parts[0]);
        String val = parts[1].substring(0, len);
        s = parts[1].substring(len);
        add(new Path(val));
      }
    }

    @Override
    public String toString() {
      StringBuilder buf = new StringBuilder();
      buf.append(size());
      buf.append(':');
      if (size() > 0) {
        for (Path p : this) {
          String pStr = p.toString();
          buf.append(pStr.length());
          buf.append(':');
          buf.append(pStr);
        }
      }
      return buf.toString();
    }
  }

  private static <T> T instantiate(Class<T> classType, String classname) throws IOException {
    T t;
    try {
      Class<?> c = JavaUtils.loadClass(classname);
      Object o = c.newInstance();
      if (classType.isAssignableFrom(o.getClass())) {
        t = (T)o;
      } else {
        String s = classname + " is not an instance of " + classType.getName();
        LOG.error(s);
        throw new IOException(s);
      }
    } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
      LOG.error("Unable to instantiate class", e);
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
      Path tmpLocation = new Path(conf.get(TMP_LOCATION));//this contains base_xxx or delta_xxx_yyy
      Path finalLocation = new Path(conf.get(FINAL_LOCATION));
      FileSystem fs = tmpLocation.getFileSystem(conf);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Moving contents of {} to {}", tmpLocation, finalLocation);
      }
      if(!fs.exists(tmpLocation)) {
        /*
         * No 'tmpLocation' may happen if job generated created 0 splits, which happens if all
         * input delta and/or base files were empty or had
         * only {@link org.apache.orc.impl.OrcAcidUtils#getSideFile(Path)} files.
         * So make sure the new base/delta is created.
         */
        AcidOutputFormat.Options options = new AcidOutputFormat.Options(conf)
            .writingBase(conf.getBoolean(IS_MAJOR, false))
            .isCompressed(conf.getBoolean(IS_COMPRESSED, false))
            .minimumWriteId(conf.getLong(MIN_TXN, Long.MAX_VALUE))
            .maximumWriteId(conf.getLong(MAX_TXN, Long.MIN_VALUE))
            .bucket(0)
            .statementId(-1)
            .visibilityTxnId(Compactor.getCompactorTxnId(conf));
        Path newDeltaDir = AcidUtils.createFilename(finalLocation, options).getParent();
        if (LOG.isInfoEnabled()) {
          LOG.info("{}: {} not found.  Assuming 0 splits.  Creating {}", context.getJobID(), tmpLocation, newDeltaDir);
        }
        fs.mkdirs(newDeltaDir);
        if (options.isWriteVersionFile()) {
          AcidUtils.OrcAcidVersion.writeVersionFile(newDeltaDir, fs);
        }
        return;
      }
      FileStatus[] contents = fs.listStatus(tmpLocation);
      AcidOutputFormat.Options options = new AcidOutputFormat.Options(conf);
      //minor compaction may actually have delta_x_y and delete_delta_x_y
      for (FileStatus fileStatus : contents) {
        //newPath is the base/delta dir
        Path newPath = new Path(finalLocation, fileStatus.getPath().getName());
        /*rename(A, B) has "interesting" behavior if A and B are directories. If  B doesn't exist,
        * it does the expected operation and everything that was in A is now in B.  If B exists,
        * it will make A a child of B...  thus make sure the rename() is done before creating the
        * meta files which will create base_x/ (i.e. B)...*/
        fs.rename(fileStatus.getPath(), newPath);
        if (options.isWriteVersionFile()) {
          AcidUtils.OrcAcidVersion.writeVersionFile(newPath, fs);
        }
      }
      fs.delete(tmpLocation, true);
    }

    @Override
    public void abortJob(JobContext context, int status) throws IOException {
      JobConf conf = ShimLoader.getHadoopShims().getJobConf(context);
      Path tmpLocation = new Path(conf.get(TMP_LOCATION));
      FileSystem fs = tmpLocation.getFileSystem(conf);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Removing {}", tmpLocation);
      }
      fs.delete(tmpLocation, true);
    }
  }
}
