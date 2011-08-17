package org.apache.hadoop.hive.cassandra.input;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;

import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.db.SuperColumn;
import org.apache.cassandra.hadoop.ColumnFamilyInputFormat;
import org.apache.cassandra.hadoop.ColumnFamilySplit;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.cassandra.serde.StandardColumnSerDe;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

@SuppressWarnings("deprecation")
public class HiveCassandraStandardColumnInputFormat extends
    ColumnFamilyInputFormat implements InputFormat<Text, HiveCassandraStandardRowResult> {

  static final Log LOG = LogFactory.getLog(HiveCassandraStandardColumnInputFormat.class);

  private boolean isTransposed;
  private Map.Entry<ByteBuffer, IColumn> currentEntry;
  private Iterator<IColumn> subcolumnIterator;

  @Override
  public RecordReader<Text, HiveCassandraStandardRowResult> getRecordReader(InputSplit split,
      JobConf jobConf, final Reporter reporter) throws IOException {
    HiveCassandraStandardSplit cassandraSplit = (HiveCassandraStandardSplit) split;

    List<String> columns = StandardColumnSerDe
        .parseColumnMapping(cassandraSplit.getColumnMapping());
    isTransposed = StandardColumnSerDe.isTransposed(columns);


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
      //We are reading all columns
      SliceRange range = new SliceRange();
      range.setStart(new byte[0]);
      range.setFinish(new byte[0]);
      range.setReversed(false);
      range.setCount(cassandraSplit.getSlicePredicateSize());
      predicate.setSlice_range(range);
    } else {
      int iKey = columns.indexOf(StandardColumnSerDe.CASSANDRA_KEY_COLUMN);
      predicate.setColumn_names(getColumnNames(iKey, columns, readColIDs));
    }

    final org.apache.hadoop.mapreduce.RecordReader<ByteBuffer, SortedMap<ByteBuffer, IColumn>> recordReader = createRecordReader(
        cfSplit, tac);

    try {
      ConfigHelper.setInputColumnFamily(tac.getConfiguration(),
          cassandraSplit.getKeyspace(), cassandraSplit.getColumnFamily());

      ConfigHelper.setInputSlicePredicate(tac.getConfiguration(), predicate);
      ConfigHelper.setRangeBatchSize(tac.getConfiguration(), cassandraSplit.getRangeBatchSize());
      ConfigHelper.setRpcPort(tac.getConfiguration(), cassandraSplit.getPort() + "");
      ConfigHelper.setInitialAddress(tac.getConfiguration(), cassandraSplit.getHost());
      ConfigHelper.setPartitioner(tac.getConfiguration(), cassandraSplit.getPartitioner());
      //Set Split Size
      ConfigHelper.setInputSplitSize(tac.getConfiguration(), cassandraSplit.getSplitSize());

      recordReader.initialize(cfSplit, tac);
    } catch (InterruptedException ie) {
      throw new IOException(ie);
    } catch (Exception ie) {
      throw new IOException(ie);
    }
    return new RecordReader<Text, HiveCassandraStandardRowResult>() {
      private Iterator<Map.Entry<ByteBuffer, IColumn>> currentRecordIterator;

      @Override
      public void close() throws IOException {
        recordReader.close();
      }

      @Override
      public Text createKey() {
        return new Text();
      }

      @Override
      public HiveCassandraStandardRowResult createValue() {
        return new HiveCassandraStandardRowResult();
      }

      @Override
      public long getPos() throws IOException {
        return 0l;
      }

      @Override
      public float getProgress() throws IOException {
        float progress = 0.0F;
        try {
          progress = recordReader.getProgress();
        } catch (InterruptedException e) {
          throw new IOException(e);
        }
        return progress;
      }

      @Override
      public boolean next(Text rowKey, HiveCassandraStandardRowResult value) throws IOException {
        boolean next = false;
        try {

          // In the case that we are transposing we create a fixed set of columns
          // per cassandra column
          if (isTransposed) {
            if (currentRecordIterator == null || !currentRecordIterator.hasNext()) {
              next = recordReader.nextKeyValue();
              if (next) {
                currentRecordIterator = recordReader.getCurrentValue().entrySet().iterator();
                subcolumnIterator = null;
                currentEntry = null;
              } else {
                //More sub columns for super columns.
                if (subcolumnIterator != null && subcolumnIterator.hasNext()) {
                  next = true;
                }
              }
            } else {
              next = true;
            }

            if (next) {
              rowKey.set(ByteBufferUtil.getArray(recordReader.getCurrentKey()));
              MapWritable theMap = new MapWritable();
              Map.Entry<ByteBuffer, IColumn> entry = currentEntry;
              if (subcolumnIterator == null || !subcolumnIterator.hasNext()) {
                entry = currentRecordIterator.next();
                currentEntry = entry;
                subcolumnIterator = null;
              }

              //is this a super column
              boolean superColumn = entry.getValue() instanceof SuperColumn;

              // Column name
              HiveIColumn hic = new HiveIColumn();
              hic.setName(StandardColumnSerDe.CASSANDRA_COLUMN_COLUMN.getBytes());
              hic.setValue(ByteBufferUtil.getArray(entry.getValue().name()));
              if (!superColumn) {
                hic.setTimestamp(entry.getValue().timestamp());
              }

              theMap.put(new BytesWritable(StandardColumnSerDe.CASSANDRA_COLUMN_COLUMN.getBytes()),
                  hic);

              // SubColumn?
              if (superColumn) {
                if (subcolumnIterator == null) {
                  subcolumnIterator = ((SuperColumn) entry.getValue()).getSubColumns().iterator();
                }

                IColumn subCol = subcolumnIterator.next();

                // Subcolumn name
                hic = new HiveIColumn();
                hic.setName(StandardColumnSerDe.CASSANDRA_SUBCOLUMN_COLUMN.getBytes());
                hic.setValue(ByteBufferUtil.getArray(subCol.name()));
                hic.setTimestamp(subCol.timestamp());

                theMap.put(new BytesWritable(StandardColumnSerDe.CASSANDRA_SUBCOLUMN_COLUMN
                    .getBytes()), hic);

                // Value
                hic = new HiveIColumn();
                hic.setName(StandardColumnSerDe.CASSANDRA_VALUE_COLUMN.getBytes());
                hic.setValue(ByteBufferUtil.getArray(subCol.value()));
                hic.setTimestamp(subCol.timestamp());

                theMap.put(
                    new BytesWritable(StandardColumnSerDe.CASSANDRA_VALUE_COLUMN.getBytes()), hic);

              } else {

                // Value
                hic = new HiveIColumn();
                hic.setName(StandardColumnSerDe.CASSANDRA_VALUE_COLUMN.getBytes());
                hic.setValue(ByteBufferUtil.getArray(entry.getValue().value()));
                hic.setTimestamp(entry.getValue().timestamp());

                theMap.put(
                    new BytesWritable(StandardColumnSerDe.CASSANDRA_VALUE_COLUMN.getBytes()), hic);

              }


              // Done
              value.setKey(rowKey);
              value.setValue(theMap);
            }


          } else {

            next = recordReader.nextKeyValue();
            if (next) {
              rowKey.set(ByteBufferUtil.getArray(recordReader.getCurrentKey()));
              MapWritable theMap = new MapWritable();
              for (Map.Entry<ByteBuffer, IColumn> entry : recordReader.getCurrentValue().entrySet()) {
                HiveIColumn hic = new HiveIColumn();
                byte[] name = ByteBufferUtil.getArray(entry.getValue().name());
                hic.setName(name);
                hic.setValue(ByteBufferUtil.getArray(entry.getValue().value()));
                hic.setTimestamp(entry.getValue().timestamp());
                theMap.put(new BytesWritable(name), hic);
              }
              value.setKey(rowKey);
              value.setValue(theMap);
            }
          }
        } catch (InterruptedException e) {
          throw new IOException(e);
        }
        return next;
      }
    };

  }

  /**
   * The Cassandra record Reader throws InteruptedException,
   * we overlay here to throw IOException instead.
   */
  @Override
  public org.apache.hadoop.mapreduce.RecordReader<ByteBuffer, SortedMap<ByteBuffer, IColumn>> createRecordReader(
      org.apache.hadoop.mapreduce.InputSplit inputSplit,
      TaskAttemptContext taskAttemptContext)
      throws IOException {
    org.apache.hadoop.mapreduce.RecordReader<ByteBuffer, SortedMap<ByteBuffer, IColumn>> result = null;
    try {
      result = super.createRecordReader(inputSplit, taskAttemptContext);
    } catch (InterruptedException e) {
      throw new IOException(e);
    } catch (Exception ex) {
      throw new IOException(ex);
    }
    return result;
  }

  @Override
  public InputSplit[] getSplits(JobConf jobConf, int numSplits) throws IOException {
    String ks = jobConf.get(StandardColumnSerDe.CASSANDRA_KEYSPACE_NAME);
    String cf = jobConf.get(StandardColumnSerDe.CASSANDRA_CF_NAME);
    int slicePredicateSize = jobConf.getInt(StandardColumnSerDe.CASSANDRA_SLICE_PREDICATE_SIZE,
        StandardColumnSerDe.DEFAULT_SLICE_PREDICATE_SIZE);
    int sliceRangeSize = jobConf.getInt(
        StandardColumnSerDe.CASSANDRA_RANGE_BATCH_SIZE,
        StandardColumnSerDe.DEFAULT_RANGE_BATCH_SIZE);
    int splitSize = jobConf.getInt(
        StandardColumnSerDe.CASSANDRA_SPLIT_SIZE,
        StandardColumnSerDe.DEFAULT_SPLIT_SIZE);
    String cassandraColumnMapping = jobConf.get(StandardColumnSerDe.CASSANDRA_COL_MAPPING);
    int rpcPort = jobConf.getInt(StandardColumnSerDe.CASSANDRA_PORT, 9160);
    String host = jobConf.get(StandardColumnSerDe.CASSANDRA_HOST);
    String partitioner = jobConf.get(StandardColumnSerDe.CASSANDRA_PARTITIONER);

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
   * Return a list of columns names to read from cassandra. The column defined as the key in the column mapping
   * should be skipped.
   *
   * @param iKey the index of the key defined in the column mappping
   * @param columns column mapping
   * @param readColIDs column names to read from cassandra
   */
  private List<ByteBuffer> getColumnNames(int iKey, List<String> columns, List<Integer> readColIDs) {

    List<ByteBuffer> results = new ArrayList();
    int maxSize = columns.size();

    for (Integer i : readColIDs) {
      assert(i < maxSize);
      if (i != iKey) {
        results.add(ByteBufferUtil.bytes(columns.get(i.intValue())));
      }
    }

    return results;
  }
}
