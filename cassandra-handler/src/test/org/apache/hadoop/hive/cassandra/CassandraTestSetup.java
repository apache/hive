package org.apache.hadoop.hive.cassandra;

import java.io.IOException;

import junit.extensions.TestSetup;
import junit.framework.Test;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.cassandra.contrib.utils.service.CassandraServiceDataCleaner;
import org.apache.cassandra.hadoop.ColumnFamilyInputFormat;
import org.apache.cassandra.service.EmbeddedCassandraService;
import org.apache.cassandra.thrift.Cassandra;
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
  void preTest(HiveConf conf) throws IOException, TTransportException, TException, CassandraException {
    if (cassandra==null){
      CassandraServiceDataCleaner cleaner = new CassandraServiceDataCleaner();
      cleaner.prepare();
      cassandra = new EmbeddedCassandraService();
      cassandra.start();
    }

    //Make sure that this server is connectable.
    Cassandra.Iface client = (Cassandra.Iface) CassandraProxyClient.newProxyConnection(
        "127.0.0.1", 9170, true, true);

    client.describe_cluster_name();


    String auxJars = conf.getAuxJars();
    auxJars = ((auxJars == null) ? "" : (auxJars + ",")) + "file://"
      + new JobConf(conf, Cassandra.Client.class).getJar();
    auxJars += ",file://" + new JobConf(conf, ColumnFamilyInputFormat.class).getJar();
    auxJars += ",file://" + new JobConf(conf, StandardColumnSerDe.class).getJar();
    auxJars += ",file://" + new JobConf(conf, org.apache.thrift.transport.TSocket.class).getJar();
    auxJars += ",file://" + new JobConf(conf, com.google.common.collect.AbstractIterator.class).getJar();
    auxJars += ",file://" + new JobConf(conf, org.apache.commons.lang.ArrayUtils.class).getJar();
    auxJars += ",file://" + new JobConf(conf, org.apache.thrift.meta_data.FieldValueMetaData.class).getJar();
    conf.setAuxJars(auxJars);

    System.err.println(auxJars);

  }

  @Override
  protected void tearDown() throws Exception {
    //do we need this?
    CassandraServiceDataCleaner cleaner = new CassandraServiceDataCleaner();
    cleaner.prepare();
  }

}

