package org.apache.hadoop.hive.cassandra.output;

import java.io.IOException;
import java.util.Properties;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.cassandra.CassandraException;
import org.apache.hadoop.hive.cassandra.CassandraProxyClient;
import org.apache.hadoop.hive.cassandra.serde.StandardColumnSerDe;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.util.Progressable;
import org.apache.thrift.TException;

@SuppressWarnings("deprecation")
public class HiveCassandraOutputFormat implements HiveOutputFormat<Text, CassandraPut>,
    OutputFormat<Text, CassandraPut> {

  static final Log LOG = LogFactory.getLog(HiveCassandraOutputFormat.class);

  @Override
  public RecordWriter getHiveRecordWriter(JobConf jc, Path finalOutPath,
      Class<? extends Writable> valueClass, boolean isCompressed, Properties tableProperties,
      Progressable progress) throws IOException {

    final String cassandraKeySpace = jc.get(StandardColumnSerDe.CASSANDRA_KEYSPACE_NAME);
    final String cassandraHost = jc.get(StandardColumnSerDe.CASSANDRA_HOST);
    final int cassandraPort = Integer.parseInt(jc.get(StandardColumnSerDe.CASSANDRA_PORT));
    final String consistencyLevel = jc.get(StandardColumnSerDe.CASSANDRA_CONSISTENCY_LEVEL,
      StandardColumnSerDe.DEFAULT_CONSISTENCY_LEVEL);
    ConsistencyLevel level = null;
    final CassandraProxyClient client;
    try {
      client = new CassandraProxyClient(
        cassandraHost, cassandraPort, true, true);
    } catch (CassandraException e) {
      throw new IOException(e);
    }

    try {
      level = ConsistencyLevel.valueOf(consistencyLevel);
    } catch (IllegalArgumentException e) {
      level = ConsistencyLevel.ONE;
    }

    final ConsistencyLevel fLevel = level;

    return new RecordWriter() {

      @Override
      public void close(boolean abort) throws IOException {
        if (client != null) {
          client.close();
        }
      }

      @Override
      public void write(Writable w) throws IOException {
        CassandraPut put = (CassandraPut) w;

        for (CassandraColumn c : put.getColumns()) {
          ColumnParent parent = new ColumnParent();
          parent.setColumn_family(c.getColumnFamily());
          try {
            Column col = new Column();
            col.setValue(c.getValue());
            col.setTimestamp(c.getTimeStamp());
            col.setName(c.getColumn());
            client.getProxyConnection().set_keyspace(cassandraKeySpace);
            client.getProxyConnection().insert(put.getKey(), parent, col, fLevel);
          } catch (InvalidRequestException e) {
            throw new IOException(e);
          } catch (UnavailableException e) {
            throw new IOException(e);
          } catch (TimedOutException e) {
            throw new IOException(e);
          } catch (TException e) {
            throw new IOException(e);
          }
        }
      }

    };
  }

  @Override
  public void checkOutputSpecs(FileSystem arg0, JobConf jc) throws IOException {

  }

  @Override
  public org.apache.hadoop.mapred.RecordWriter<Text, CassandraPut> getRecordWriter(FileSystem arg0,
      JobConf arg1, String arg2, Progressable arg3) throws IOException {
    throw new RuntimeException("Error: Hive should not invoke this method.");
  }
}
