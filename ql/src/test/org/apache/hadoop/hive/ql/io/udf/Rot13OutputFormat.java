package org.apache.hadoop.hive.ql.io.udf;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;
import java.util.Properties;

public class Rot13OutputFormat
  extends HiveIgnoreKeyTextOutputFormat<LongWritable,Text> {

  @Override
  public RecordWriter
    getHiveRecordWriter(JobConf jc,
                        Path outPath,
                        Class<? extends Writable> valueClass,
                        boolean isCompressed,
                        Properties tableProperties,
                        Progressable progress) throws IOException {
    final RecordWriter result =
      super.getHiveRecordWriter(jc,outPath,valueClass,isCompressed,
        tableProperties,progress);
    final Reporter reporter = (Reporter) progress;
    reporter.setStatus("got here");
    System.out.println("Got a reporter " + reporter);
    return new RecordWriter() {
      @Override
      public void write(Writable w) throws IOException {
        if (w instanceof Text) {
          Text value = (Text) w;
          Rot13InputFormat.rot13(value.getBytes(), 0, value.getLength());
          result.write(w);
        } else if (w instanceof BytesWritable) {
          BytesWritable value = (BytesWritable) w;
          Rot13InputFormat.rot13(value.getBytes(), 0, value.getLength());
          result.write(w);
        } else {
          throw new IllegalArgumentException("need text or bytes writable " +
            " instead of " + w.getClass().getName());
        }
      }

      @Override
      public void close(boolean abort) throws IOException {
        result.close(abort);
      }
    };
  }
}
