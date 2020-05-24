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
package org.apache.hadoop.hive.kudu;

import static org.apache.hadoop.hive.kudu.KuduStorageHandler.KUDU_TABLE_NAME_KEY;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.utils.StringUtils;
import org.apache.hadoop.hive.ql.metadata.VirtualColumn;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.kudu.client.Bytes;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduPredicate;
import org.apache.kudu.client.KuduScanToken;
import org.apache.kudu.client.KuduScanner;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.LocatedTablet;
import org.apache.kudu.client.RowResult;

/**
 * A Kudu InputFormat implementation for use by Hive.
 */
public class KuduInputFormat extends InputFormat<NullWritable, KuduWritable>
    implements org.apache.hadoop.mapred.InputFormat<NullWritable, KuduWritable> {

  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException {
    return computeSplits(context.getConfiguration()).stream()
        .map(is -> (InputSplit) is)
        .collect(Collectors.toList());
  }

  @Override
  public org.apache.hadoop.mapred.InputSplit[] getSplits(JobConf conf, int numSplits)
      throws IOException {
    List<KuduInputSplit> splits = computeSplits(conf);
    return splits.toArray(new org.apache.hadoop.mapred.InputSplit[0]);
  }

  private List<KuduInputSplit> computeSplits(Configuration conf) throws IOException {
    try (KuduClient client = KuduHiveUtils.getKuduClient(conf)) {
      // Hive depends on FileSplits so we get the dummy Path for the Splits.
      Job job = Job.getInstance(conf);
      JobContext jobContext = ShimLoader.getHadoopShims().newJobContext(job);
      Path[] paths = FileInputFormat.getInputPaths(jobContext);
      Path dummyPath = paths[0];

      String tableName = conf.get(KUDU_TABLE_NAME_KEY);
      if (StringUtils.isEmpty(tableName)) {
        throw new IllegalArgumentException(KUDU_TABLE_NAME_KEY + " is not set.");
      }
      if (!client.tableExists(tableName)) {
        throw new IllegalArgumentException("Kudu table does not exist: " + tableName);
      }

      KuduTable table = client.openTable(tableName);
      List<KuduPredicate> predicates = KuduPredicateHandler.getPredicates(conf, table.getSchema());

      KuduScanToken.KuduScanTokenBuilder tokenBuilder = client.newScanTokenBuilder(table)
          .setProjectedColumnNames(getProjectedColumns(conf));

      for (KuduPredicate predicate : predicates) {
        tokenBuilder.addPredicate(predicate);
      }
      List<KuduScanToken> tokens = tokenBuilder.build();

      List<KuduInputSplit> splits = new ArrayList<>(tokens.size());
      for (KuduScanToken token : tokens) {
        List<String> locations = new ArrayList<>(token.getTablet().getReplicas().size());
        for (LocatedTablet.Replica replica : token.getTablet().getReplicas()) {
          locations.add(replica.getRpcHost());
        }
        splits.add(new KuduInputSplit(token, dummyPath, locations.toArray(new String[0])));
      }
      return splits;
    }
  }

  private List<String> getProjectedColumns(Configuration conf) throws IOException {
    String[] columnNamesArr = conf.getStrings(serdeConstants.LIST_COLUMNS);
    if (null == columnNamesArr) {
      throw new IOException(
          "Hive column names must be provided to InputFormat in the Configuration");
    }
    List<String> columns = new ArrayList<>(Arrays.asList(columnNamesArr));
    VirtualColumn.removeVirtualColumns(columns);
    return columns;
  }

  @Override
  public RecordReader<NullWritable, KuduWritable> createRecordReader(InputSplit split,
                                                                     TaskAttemptContext context) {
    Preconditions.checkArgument(split instanceof KuduInputSplit);
    // Will be initialized via the initialize method.
    return new KuduRecordReader();
  }

  @Override
  public org.apache.hadoop.mapred.RecordReader<NullWritable, KuduWritable> getRecordReader(
      org.apache.hadoop.mapred.InputSplit split, JobConf conf, Reporter reporter)
      throws IOException {
    Preconditions.checkArgument(split instanceof KuduInputSplit);
    KuduRecordReader recordReader = new KuduRecordReader();
    recordReader.initialize((KuduInputSplit) split, conf);
    return recordReader;
  }

  /**
   * An InputSplit represents the data to be processed by an individual Mapper.
   * This is effectively a wrapper around a Kudu scan token.
   */
  static class KuduInputSplit extends FileSplit implements org.apache.hadoop.mapred.InputSplit {
    /** The scan token that the split will use to scan the Kudu table. */
    private byte[] serializedScanToken;

    /** Tablet server locations which host the tablet to be scanned. */
    private String[] locations;

    @SuppressWarnings("unused") // required for deserialization.
    KuduInputSplit() {
      super(null, 0, 0, (String[]) null);
    }

    KuduInputSplit(KuduScanToken token, Path dummyPath, String[] locations) throws IOException {
      super(dummyPath, 0, 0, locations);
      this.serializedScanToken = token.serialize();
      this.locations = locations;
    }

    byte[] getSerializedScanToken() {
      return serializedScanToken;
    }

    @Override
    public long getLength() {
      return 0;
    }

    @Override
    public String[] getLocations() {
      return locations;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      super.readFields(in);
      serializedScanToken = Bytes.readByteArray(in);
      locations = new String[in.readInt()];
      for (int i = 0; i < locations.length; i++) {
        byte[] str = Bytes.readByteArray(in);
        locations[i] = Bytes.getString(str);
      }
    }

    @Override
    public void write(DataOutput out) throws IOException {
      super.write(out);
      Bytes.writeByteArray(out, serializedScanToken);
      out.writeInt(locations.length);
      for (String location : locations) {
        byte[] str = Bytes.fromString(location);
        Bytes.writeByteArray(out, str);
      }
    }
  }

  /**
   * A RecordReader that reads the Kudu rows from a KuduInputSplit.
   */
  static class KuduRecordReader extends RecordReader<NullWritable, KuduWritable>
      implements org.apache.hadoop.mapred.RecordReader<NullWritable, KuduWritable> {

    private volatile boolean initialized = false;
    private KuduClient client;
    private KuduScanner scanner;
    private Iterator<RowResult> iterator;
    private RowResult currentValue;
    private KuduWritable currentWritable;
    private long pos;

    KuduRecordReader() {}

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException {
      Preconditions.checkArgument(split instanceof KuduInputSplit);
      initialize((KuduInputSplit) split, context.getConfiguration());
    }

    private synchronized void initialize(KuduInputSplit split, Configuration conf)
        throws IOException {
      if (!initialized) {
        byte[] serializedScanToken = split.getSerializedScanToken();
        client = KuduHiveUtils.getKuduClient(conf);
        scanner = KuduScanToken.deserializeIntoScanner(serializedScanToken, client);
        iterator = scanner.iterator();
        currentValue = null;
        currentWritable = new KuduWritable(scanner.getProjectionSchema().newPartialRow());
        pos = 0;
        initialized = true;
      }
    }

    @Override
    public boolean nextKeyValue() {
      if (iterator.hasNext()) {
        currentValue = iterator.next();
        currentWritable.setRow(currentValue);
        pos++;
        return true;
      }
      currentValue = null;
      return false;
    }

    @Override
    public NullWritable getCurrentKey() {
      return NullWritable.get();
    }

    @Override
    public KuduWritable getCurrentValue() {
      Preconditions.checkNotNull(currentValue);
      return currentWritable;
    }

    @Override
    public boolean next(NullWritable nullWritable, KuduWritable kuduWritable) {
      if (nextKeyValue()) {
        kuduWritable.setRow(currentValue);
        return true;
      }
      return false;
    }

    @Override public NullWritable createKey() {
      return NullWritable.get();
    }

    @Override public KuduWritable createValue() {
      return new KuduWritable(scanner.getProjectionSchema().newPartialRow());
    }

    @Override
    public void close() throws IOException {
      try {
        scanner.close();
      } catch (KuduException e) {
        throw new IOException(e);
      }
      client.shutdown();
    }

    @Override
    public long getPos() {
      return pos;
    }

    @Override
    public float getProgress() {
      return 0;
    }
  }
}
