package org.apache.hadoop.hive.cassandra;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.commons.lang.StringUtils;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CassandraClientHolder implements ClientHolder
{
    private static final Logger log = LoggerFactory.getLogger(CassandraClientHolder.class);

    private Cassandra.Client client;
    private final TTransport transport;
    private String keyspace;

    public CassandraClientHolder(TTransport transport) throws CassandraException
    {
        this(transport, null);
    }

    public CassandraClientHolder(TTransport transport, String keyspace) throws CassandraException

    {
        this.transport = transport;
        this.keyspace = keyspace;
        initClient();
    }

    public boolean isOpen()
    {
        return client != null && transport != null && transport.isOpen();
    }

    public String getKeyspace()
    {
        return keyspace;
    }

    private void initClient() throws CassandraException
    {
        try
        {
            transport.open();
        } catch (TTransportException e)
        {
            throw new CassandraException("unable to connect to server", e);
        }

        client = new Cassandra.Client(new TBinaryProtocol(transport));

        // connect to last known keyspace
        maybeSetKeyspace(keyspace);
    }

    private void maybeSetKeyspace(String ks) throws CassandraException
    {
        if ( ks == null )
        {
            return;
        }

        if (keyspace == null || !StringUtils.equals(keyspace, ks))
        {
            try
            {
                this.keyspace = ks;
                client.set_keyspace(keyspace);
            } catch (InvalidRequestException e)
            {
                throw new CassandraException(e);
            } catch (TException e)
            {
                throw new CassandraException(e);
            }
        }
    }

    public Cassandra.Client getClient()
    {
        return client;
    }


    public Cassandra.Client getClient(String keyspace) throws CassandraException
    {
        maybeSetKeyspace(keyspace);
        return client;
    }



    public void close()
    {
        if ( transport == null || !transport.isOpen() )
        {
            return;
        }
        try
        {
            transport.flush();
        } catch (Exception e)
        {
            log.error("Could not flush transport for client holder: "+ toString(), e);
        } finally
        {
            try
            {
                if (transport.isOpen())
                {
                    transport.close();
                }
            } catch (Exception e)
            {
                log.error("Error on transport close for client: " + toString(),e);
            }
        }
    }

}
