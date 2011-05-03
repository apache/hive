package org.apache.hadoop.hive.cassandra;

import org.apache.cassandra.thrift.Cassandra;

public interface ClientHolder
{
    boolean isOpen();

    String getKeyspace();

    Cassandra.Client getClient();

    /**
     * Return the client with the (potentially) new keyspace. Safe to call this
     * repeatedly with the same keyspace.
     * @param keyspace
     * @return
     * @throws CassandraException
     */
    Cassandra.Client getClient(String keyspace)
            throws CassandraException;

    void close();

}