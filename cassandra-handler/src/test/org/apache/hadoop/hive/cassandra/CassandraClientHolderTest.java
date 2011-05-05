package org.apache.hadoop.hive.cassandra;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class CassandraClientHolderTest extends BaseCassandraConnectionTest {
    private final Logger log = LoggerFactory.getLogger(CassandraClientHolderTest.class);

    public void testBuildClientHolder() throws Exception
    {
        maybeStartServer();
        TSocket socket = new TSocket("127.0.0.1", 9170);
        TTransport trans = new TFramedTransport(socket);

        CassandraClientHolder clientHolder = new CassandraClientHolder(trans, ksName);

        assertEquals("Test Cluster",clientHolder.getClient().describe_cluster_name());

    }

    public void testIsOpen()
    {
        try {
            maybeStartServer();
            TSocket socket = new TSocket("127.0.0.1", 9170);
            TTransport trans = new TFramedTransport(socket);
            CassandraClientHolder clientHolder = new CassandraClientHolder(trans, ksName);
            assertTrue(clientHolder.isOpen());
            clientHolder.close();
            assertFalse(clientHolder.isOpen());
        } catch (Exception ex) {
            log.error("",ex);
            fail();
        }
    }

    public void testSetKeyspace() throws Exception
    {
        maybeStartServer();
        TSocket socket = new TSocket("127.0.0.1", 9170);
        TTransport trans = new TFramedTransport(socket);
        CassandraClientHolder clientHolder = new CassandraClientHolder(trans);
        assertNull(clientHolder.getKeyspace());
        clientHolder.setKeyspace(ksName);
        Cassandra.Client client = clientHolder.getClient();
        assertNotNull(client);
        assertEquals(ksName, clientHolder.getKeyspace());
    }
}
