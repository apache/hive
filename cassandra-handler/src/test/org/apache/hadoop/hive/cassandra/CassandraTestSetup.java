package org.apache.hadoop.hive.cassandra;

import junit.extensions.TestSetup;
import junit.framework.Test;

import org.apache.cassandra.contrib.utils.service.CassandraEmbeddedTestSetup;
import org.apache.cassandra.contrib.utils.service.CassandraServiceFactory;
import org.apache.cassandra.hadoop.ColumnFamilyInputFormat;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.cassandra.serde.AbstractColumnSerDe;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.mapred.JobConf;

public class CassandraTestSetup extends TestSetup {

  static final Log LOG = LogFactory.getLog(CassandraTestSetup.class);
  private CassandraEmbeddedTestSetup cassandra;


  public CassandraTestSetup(Test test) {
    super(test);
  }


  @SuppressWarnings("deprecation")
  void preTest(HiveConf conf, String logDir) throws Exception {
    CassandraServiceFactory csf = new CassandraServiceFactory(":", logDir+Path.SEPARATOR+"cassandra-artifacts.txt");

    if (cassandra == null) {
      cassandra = csf.getEmbeddedCassandraService();
    }


    String auxJars = conf.getAuxJars();
    auxJars = ((auxJars == null) ? "" : (auxJars + ",")) + "file://"
        + new JobConf(conf, Cassandra.Client.class).getJar();
    auxJars += ",file://" + new JobConf(conf, ColumnFamilyInputFormat.class).getJar();
    auxJars += ",file://" + new JobConf(conf, AbstractColumnSerDe.class).getJar();
    auxJars += ",file://" + new JobConf(conf, org.apache.thrift.transport.TSocket.class).getJar();
    auxJars += ",file://"
        + new JobConf(conf, com.google.common.collect.AbstractIterator.class).getJar();
    auxJars += ",file://" + new JobConf(conf, org.apache.commons.lang.ArrayUtils.class).getJar();
    auxJars += ",file://"
        + new JobConf(conf, org.apache.thrift.meta_data.FieldValueMetaData.class).getJar();
    auxJars += ",file://" + new JobConf(conf, org.cliffc.high_scale_lib.NonBlockingHashMap.class).getJar();
    conf.setAuxJars(auxJars);
  }

  @Override
  protected void tearDown() throws Exception {
    if (cassandra != null)
    {
      cassandra.stop();
      cassandra = null;
    }
  }

}
