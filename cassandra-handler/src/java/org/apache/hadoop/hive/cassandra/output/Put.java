package org.apache.hadoop.hive.cassandra.output;

import java.io.IOException;

import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.hadoop.hive.cassandra.CassandraProxyClient;

public interface Put {
  void write(String keySpace, CassandraProxyClient client, ConsistencyLevel flevel) throws IOException;
}
