package org.apache.hadoop.hive.llap;

import java.io.IOException;

import org.apache.hadoop.hive.llap.LlapBaseRecordReader;
import org.apache.hadoop.hive.llap.LlapInputSplit;
import org.apache.hadoop.hive.llap.LlapRowRecordReader;
import org.apache.hadoop.hive.llap.Row;
import org.apache.hadoop.hive.llap.Schema;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;


public class LlapRowInputFormat implements InputFormat<NullWritable, Row> {
  LlapBaseInputFormat<Text> baseInputFormat = new LlapBaseInputFormat<Text>();

  @Override
  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
    return baseInputFormat.getSplits(job, numSplits);
  }

  @Override
  public RecordReader<NullWritable, Row> getRecordReader(InputSplit split, JobConf job, Reporter reporter)
      throws IOException {
    LlapInputSplit llapSplit = (LlapInputSplit) split;
    LlapBaseRecordReader<Text> reader = (LlapBaseRecordReader<Text>) baseInputFormat.getRecordReader(llapSplit, job, reporter);
    return new LlapRowRecordReader(job, reader.getSchema(), reader);
  }
}
