package org.apache.hadoop.hive.cassandra.output;

import java.io.IOException;

import org.apache.hadoop.hive.cassandra.CassandraProxyClient;
import org.apache.hadoop.mapred.JobConf;

public interface Put {
  /**
   * Write this object into the given keyspace in cassandra using cassandra proxy client with given consistency level.
   *
   * @param keySpace key space to be written into
   * @param client cassandra proxy client
   * @param flevel consistency level
   * @param batchMutationSize batch mutation size
   * @throws IOException error writing into cassandra
   */
  void write(String keySpace, CassandraProxyClient client, JobConf jc) throws IOException;
}
