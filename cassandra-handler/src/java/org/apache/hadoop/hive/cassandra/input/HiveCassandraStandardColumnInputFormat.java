package org.apache.hadoop.hive.cassandra.input;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.TypeParser;
import org.apache.cassandra.hadoop.ColumnFamilyInputFormat;
import org.apache.cassandra.hadoop.ColumnFamilyRecordReader;
import org.apache.cassandra.hadoop.ColumnFamilySplit;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.cassandra.serde.AbstractColumnSerDe;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

@SuppressWarnings("deprecation")
public class HiveCassandraStandardColumnInputFormat extends InputFormat<BytesWritable, MapWritable>
implements org.apache.hadoop.mapred.InputFormat<BytesWritable, MapWritable> {

  static final Log LOG = LogFactory.getLog(HiveCassandraStandardColumnInputFormat.class);

  private boolean isTransposed;
  private final ColumnFamilyInputFormat cfif = new ColumnFamilyInputFormat();

  @Override
  public RecordReader<BytesWritable, MapWritable> getRecordReader(InputSplit split,
      JobConf jobConf, final Reporter reporter) throws IOException {
    HiveCassandraStandardSplit cassandraSplit = (HiveCassandraStandardSplit) split;

    List<String> columns = AbstractColumnSerDe.parseColumnMapping(cassandraSplit.getColumnMapping());
    isTransposed = AbstractColumnSerDe.isTransposed(columns);


    List<Integer> readColIDs = ColumnProjectionUtils.getReadColumnIDs(jobConf);

    if (columns.size() < readColIDs.size()) {
      throw new IOException("Cannot read more columns than the given table contains.");
    }

    org.apache.cassandra.hadoop.ColumnFamilySplit cfSplit = cassandraSplit.getSplit();
    Job job = new Job(jobConf);

    TaskAttemptContext tac = new TaskAttemptContext(job.getConfiguration(), new TaskAttemptID()) {
      @Override
      public void progress() {
        reporter.progress();
      }
    };

    SlicePredicate predicate = new SlicePredicate();

    if (isTransposed || readColIDs.size() == columns.size() || readColIDs.size() == 0) {
      SliceRange range = new SliceRange();
      AbstractType comparator = BytesType.instance;

      String comparatorType = jobConf.get(AbstractColumnSerDe.CASSANDRA_SLICE_PREDICATE_RANGE_COMPARATOR);
      if (comparatorType != null && !comparatorType.equals("")) {
        try {
          comparator = TypeParser.parse(comparatorType);
        } catch (ConfigurationException ex) {
          throw new IOException("Comparator class not found.");
        }
      }

      String sliceStart = jobConf.get(AbstractColumnSerDe.CASSANDRA_SLICE_PREDICATE_RANGE_START);
      String sliceEnd = jobConf.get(AbstractColumnSerDe.CASSANDRA_SLICE_PREDICATE_RANGE_FINISH);
      String reversed = jobConf.get(AbstractColumnSerDe.CASSANDRA_SLICE_PREDICATE_RANGE_REVERSED);

      range.setStart(comparator.fromString(sliceStart == null ? "" : sliceStart));
      range.setFinish(comparator.fromString(sliceEnd == null ? "" : sliceEnd));
      range.setReversed(reversed == null ? false : reversed.equals("true"));
      range.setCount(cassandraSplit.getSlicePredicateSize());
      predicate.setSlice_range(range);
    } else {
      int iKey = columns.indexOf(AbstractColumnSerDe.CASSANDRA_KEY_COLUMN);
      predicate.setColumn_names(getColumnNames(iKey, columns, readColIDs));
    }


    try {
      ConfigHelper.setInputColumnFamily(tac.getConfiguration(),
          cassandraSplit.getKeyspace(), cassandraSplit.getColumnFamily());

      ConfigHelper.setInputSlicePredicate(tac.getConfiguration(), predicate);
      ConfigHelper.setRangeBatchSize(tac.getConfiguration(), cassandraSplit.getRangeBatchSize());
      ConfigHelper.setRpcPort(tac.getConfiguration(), cassandraSplit.getPort() + "");
      ConfigHelper.setInitialAddress(tac.getConfiguration(), cassandraSplit.getHost());
      ConfigHelper.setPartitioner(tac.getConfiguration(), cassandraSplit.getPartitioner());
      // Set Split Size
      ConfigHelper.setInputSplitSize(tac.getConfiguration(), cassandraSplit.getSplitSize());

      CassandraHiveRecordReader rr = new CassandraHiveRecordReader(new ColumnFamilyRecordReader(), isTransposed);
      rr.initialize(cfSplit, tac);

      return rr;

    } catch (Exception ie) {
      throw new IOException(ie);
    }
  }


  @Override
  public InputSplit[] getSplits(JobConf jobConf, int numSplits) throws IOException {
    String ks = jobConf.get(AbstractColumnSerDe.CASSANDRA_KEYSPACE_NAME);
    String cf = jobConf.get(AbstractColumnSerDe.CASSANDRA_CF_NAME);
    int slicePredicateSize = jobConf.getInt(AbstractColumnSerDe.CASSANDRA_SLICE_PREDICATE_SIZE,
        AbstractColumnSerDe.DEFAULT_SLICE_PREDICATE_SIZE);
    int sliceRangeSize = jobConf.getInt(
        AbstractColumnSerDe.CASSANDRA_RANGE_BATCH_SIZE,
        AbstractColumnSerDe.DEFAULT_RANGE_BATCH_SIZE);
    int splitSize = jobConf.getInt(
        AbstractColumnSerDe.CASSANDRA_SPLIT_SIZE,
        AbstractColumnSerDe.DEFAULT_SPLIT_SIZE);
    String cassandraColumnMapping = jobConf.get(AbstractColumnSerDe.CASSANDRA_COL_MAPPING);
    int rpcPort = jobConf.getInt(AbstractColumnSerDe.CASSANDRA_PORT, 9160);
    String host = jobConf.get(AbstractColumnSerDe.CASSANDRA_HOST);
    String partitioner = jobConf.get(AbstractColumnSerDe.CASSANDRA_PARTITIONER);

    if (cassandraColumnMapping == null) {
      throw new IOException("cassandra.columns.mapping required for Cassandra Table.");
    }

    SliceRange range = new SliceRange();
    range.setStart(new byte[0]);
    range.setFinish(new byte[0]);
    range.setReversed(false);
    range.setCount(slicePredicateSize);
    SlicePredicate predicate = new SlicePredicate();
    predicate.setSlice_range(range);

    ConfigHelper.setRpcPort(jobConf, "" + rpcPort);
    ConfigHelper.setInitialAddress(jobConf, host);
    ConfigHelper.setPartitioner(jobConf, partitioner);
    ConfigHelper.setInputSlicePredicate(jobConf, predicate);
    ConfigHelper.setInputColumnFamily(jobConf, ks, cf);
    ConfigHelper.setRangeBatchSize(jobConf, sliceRangeSize);
    ConfigHelper.setInputSplitSize(jobConf, splitSize);

    Job job = new Job(jobConf);
    JobContext jobContext = new JobContext(job.getConfiguration(), job.getJobID());

    Path[] tablePaths = FileInputFormat.getInputPaths(jobContext);
    List<org.apache.hadoop.mapreduce.InputSplit> splits = getSplits(jobContext);
    InputSplit[] results = new InputSplit[splits.size()];

    for (int i = 0; i < splits.size(); ++i) {
      HiveCassandraStandardSplit csplit = new HiveCassandraStandardSplit(
          (ColumnFamilySplit) splits.get(i), cassandraColumnMapping, tablePaths[0]);
      csplit.setKeyspace(ks);
      csplit.setColumnFamily(cf);
      csplit.setRangeBatchSize(sliceRangeSize);
      csplit.setSplitSize(splitSize);
      csplit.setHost(host);
      csplit.setPort(rpcPort);
      csplit.setSlicePredicateSize(slicePredicateSize);
      csplit.setPartitioner(partitioner);
      csplit.setColumnMapping(cassandraColumnMapping);
      results[i] = csplit;
    }
    return results;
  }

  /**
   * Return a list of columns names to read from cassandra. The column defined as the key in the
   * column mapping
   * should be skipped.
   *
   * @param iKey
   *          the index of the key defined in the column mappping
   * @param columns
   *          column mapping
   * @param readColIDs
   *          column names to read from cassandra
   */
  private List<ByteBuffer> getColumnNames(int iKey, List<String> columns, List<Integer> readColIDs) {

    List<ByteBuffer> results = new ArrayList();
    int maxSize = columns.size();

    for (Integer i : readColIDs) {
      assert (i < maxSize);
      if (i != iKey) {
        results.add(ByteBufferUtil.bytes(columns.get(i.intValue())));
      }
    }

    return results;
  }

  @Override
  public List<org.apache.hadoop.mapreduce.InputSplit> getSplits(JobContext context)
      throws IOException {
    return cfif.getSplits(context);
  }


  @Override
  public org.apache.hadoop.mapreduce.RecordReader<BytesWritable, MapWritable> createRecordReader(
      org.apache.hadoop.mapreduce.InputSplit arg0, TaskAttemptContext arg1) throws IOException,
      InterruptedException {
    return new CassandraHiveRecordReader(new ColumnFamilyRecordReader(), isTransposed);
  }


}
