package org.apache.hadoop.hive.cassandra;

import java.net.URL;

import junit.extensions.TestSetup;
import junit.framework.Test;

import org.apache.cassandra.contrib.utils.service.CassandraServiceDataCleaner;
import org.apache.cassandra.contrib.utils.service.CassandraServiceFactory;
import org.apache.cassandra.contrib.utils.service.CassandraThriftClassLoader;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;

public class CassandraTestSetup extends TestSetup {

  static final Log LOG = LogFactory.getLog(CassandraTestSetup.class);
  private Object cassandra;


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
    for (URL url : csf.urls) {
      String u = url.toExternalForm();
      if (u.endsWith(".jar")) {
        if(auxJars == null || auxJars.isEmpty()) {
          auxJars = u;
        } else {
          auxJars += ","+u;
        }
      }
    }

    conf.setAuxJars(auxJars);
    conf.setClassLoader(new CassandraThriftClassLoader(csf.urls));
  }

  @Override
  protected void tearDown() throws Exception {
    // do we need this?
    CassandraServiceDataCleaner cleaner = new CassandraServiceDataCleaner();
    cleaner.prepare();
  }

}
