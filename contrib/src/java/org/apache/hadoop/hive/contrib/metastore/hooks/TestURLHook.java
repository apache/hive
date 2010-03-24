package org.apache.hadoop.hive.contrib.metastore.hooks;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.hooks.JDOConnectionURLHook;

/**
 * First returns a url for a blank DB, then returns a URL for the original DB.
 * For testing the feature in url_hook.q
 */
public class TestURLHook implements JDOConnectionURLHook {

  static String originalUrl = null;
  @Override
  public String getJdoConnectionUrl(Configuration conf) throws Exception {
    if (originalUrl == null) {
      originalUrl = conf.get(HiveConf.ConfVars.METASTORECONNECTURLKEY.varname, "");
      return "jdbc:derby:;databaseName=../build/test/junit_metastore_db_blank;create=true";
    } else {
      return originalUrl;
    }

  }

  @Override
  public void notifyBadConnectionUrl(String url) {

  }

}
