package org.apache.hadoop.hive.cassandra;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import junit.framework.TestCase;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.contrib.utils.service.CassandraServiceDataCleaner;
import org.apache.cassandra.db.ColumnFamilyType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.service.EmbeddedCassandraService;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;

public abstract class BaseCassandraConnectionTest extends TestCase {

  protected static CassandraProxyClient client;
  private static EmbeddedCassandraService cassandra;
  protected String ksName = "TestKeyspace";
  protected String cfName = "TestColumnFamily";

  /**
   * Start the embedded cassandra server if it is not up.
   *
   * @throws IOException
   * @throws TTransportException
   * @throws TException
   * @throws CassandraException
   */
  protected void maybeStartServer() throws IOException, TTransportException, TException, CassandraException
  {

      if (cassandra==null){
          CassandraServiceDataCleaner cleaner = new CassandraServiceDataCleaner();
          cleaner.prepare();
          cassandra = new EmbeddedCassandraService();
          cassandra.start();
          client = new CassandraProxyClient(
                  "127.0.0.1", 9170, true, true);
          loadSchema();
      }

      client.getProxyConnection().describe_cluster_name();
  }



  private void loadSchema()
  {
      try
      {
          for (KSMetaData ksm : schemaDefinition())
          {
              for (CFMetaData cfm : ksm.cfMetaData().values()) {

                  Schema.instance.load(cfm);
              }
              Schema.instance.setTableDefinition(ksm, Schema.instance.getVersion());
          }
      }
      catch (ConfigurationException e)
      {
          throw new RuntimeException(e);
      }
  }

  private Collection<KSMetaData> schemaDefinition()
  {
      List<KSMetaData> schema = new ArrayList<KSMetaData>();

      Class<? extends AbstractReplicationStrategy> simple = SimpleStrategy.class;

      Map<String, String> opts_rf1 = KSMetaData.optsWithRF(1);

      // initial test keyspace
      schema.add(KSMetaData.testMetadata(
              ksName,
              simple,
              opts_rf1,
              // Add more Column Families as needed
              standardCFMD(ksName, "TestColumnFamily")));


      return schema;
  }

  private static CFMetaData standardCFMD(String ksName, String cfName)
  {
      return new CFMetaData(ksName, cfName, ColumnFamilyType.Standard, BytesType.instance, null).keyCacheSize(0);
  }

}
