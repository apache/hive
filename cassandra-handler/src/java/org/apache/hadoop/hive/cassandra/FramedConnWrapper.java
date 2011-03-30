package org.apache.hadoop.hive.cassandra;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

public class FramedConnWrapper {
   private final TTransport transport;
   private final TProtocol proto;
   private final TSocket socket;
   public FramedConnWrapper(String host, int port,int timeout) {
       socket = new TSocket(host,port,timeout);
       transport = new TFramedTransport(socket);
       proto = new TBinaryProtocol(transport);
   }
   public void open() throws TTransportException  {
       transport.open();
   }
   public void close()  {
       transport.close();
       socket.close();
   }
   public Cassandra.Client getClient() {
       Cassandra.Client client = new Cassandra.Client(proto);
       return client;
   }
}
