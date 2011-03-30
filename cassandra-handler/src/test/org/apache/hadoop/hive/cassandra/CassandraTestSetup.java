package org.apache.hadoop.hive.cassandra;

import java.io.IOException;

import junit.extensions.TestSetup;
import junit.framework.Test;

import org.apache.cassandra.contrib.utils.service.CassandraServiceDataCleaner;
import org.apache.cassandra.service.EmbeddedCassandraService;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.cassandra.serde.StandardColumnSerDe;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.mapred.JobConf;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;

public class CassandraTestSetup extends TestSetup{

  static final Log LOG = LogFactory.getLog(CassandraTestSetup.class);
  private EmbeddedCassandraService cassandra;

  public CassandraTestSetup(Test test){
    super(test);
  }

  @SuppressWarnings("deprecation")
  void preTest(HiveConf conf) throws IOException, TTransportException, TException {
    if (cassandra==null){
      CassandraServiceDataCleaner cleaner = new CassandraServiceDataCleaner();
      cleaner.prepare();
      cassandra = new EmbeddedCassandraService();
      cassandra.start();
    }

    FramedConnWrapper wrap = new FramedConnWrapper("127.0.0.1",9170,5000);
    wrap.open();
    KsDef ks = new KsDef();
    ks.setName("Keyspace1");
    ks.setReplication_factor(1);
    ks.setStrategy_class("org.apache.cassandra.locator.SimpleStrategy");
    CfDef cf = new CfDef();
    cf.setName("Standard1");
    cf.setKeyspace("Keyspace1");
    ks.addToCf_defs(cf);
    Cassandra.Client client = wrap.getClient();
    try {
      try {
        KsDef exists =  client.describe_keyspace("Keyspace1");
      } catch (NotFoundException ex){
        client.system_add_keyspace(ks);
        System.out.println("ks added");
        try {
          Thread.sleep(2000);
        } catch (Exception ex2){}
      }
    } catch (InvalidRequestException e){
      throw new RuntimeException(e);
    }
    wrap.close();

    String auxJars = conf.getAuxJars();
    auxJars = ((auxJars == null) ? "" : (auxJars + ",")) + "file://"
      + new JobConf(conf, Cassandra.Client.class).getJar();
    auxJars += ",file://" + new JobConf(conf, StandardColumnSerDe.class).getJar();
    auxJars += ",file://" + new JobConf(conf, org.apache.thrift.transport.TSocket.class).getJar();
    auxJars += ",file://" + new JobConf(conf, com.google.common.collect.AbstractIterator.class).getJar();
    auxJars += ",file://" + new JobConf(conf, org.apache.commons.lang.ArrayUtils.class).getJar();
    conf.setAuxJars(auxJars);

  }

  @Override
  protected void tearDown() throws Exception {
    //do we need this?
    CassandraServiceDataCleaner cleaner = new CassandraServiceDataCleaner();
    cleaner.prepare();
  }

}

