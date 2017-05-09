package org.apache.hadoop.hive.ql.parse;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.ql.CommandNeedRetryException;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hive.hcatalog.listener.DbNotificationListener;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

class WareHouse {
  private Driver driver;
  private HiveMetaStoreClient client;
  private HiveConf hconf;

  private final static String LISTENER_CLASS = DbNotificationListener.class.getCanonicalName();

  /**
   * This will be used to allow the primary and replica warehouse to be the same instance of
   * hive server
   */
  WareHouse(WareHouse other){
    this.driver = other.driver;
    this.client = other.client;
    this.hconf = other.hconf;
  }

  WareHouse() throws Exception {
    hconf = new HiveConf(TestReplicationScenarios.class);
    String metaStoreUri = System.getProperty("test." + HiveConf.ConfVars.METASTOREURIS.varname);
    String hiveWarehouseLocation = System.getProperty("test.warehouse.dir", "/tmp")
        + Path.SEPARATOR
        + TestReplicationScenarios.class.getCanonicalName().replace('.', '_')
        + "_"
        + System.currentTimeMillis();

    if (metaStoreUri != null) {
      hconf.setVar(HiveConf.ConfVars.METASTOREURIS, metaStoreUri);
      //        useExternalMS = true;
      return;
    }

    // turn on db notification listener on meta store
    hconf.setVar(HiveConf.ConfVars.METASTORE_TRANSACTIONAL_EVENT_LISTENERS, LISTENER_CLASS);
    hconf.setBoolVar(HiveConf.ConfVars.REPLCMENABLED, true);
    hconf.setBoolVar(HiveConf.ConfVars.FIRE_EVENTS_FOR_DML, true);
    hconf.setVar(HiveConf.ConfVars.REPLCMDIR, hiveWarehouseLocation + "/cmroot/");
    int metaStorePort = MetaStoreUtils.startMetaStore(hconf);
    hconf.setVar(HiveConf.ConfVars.REPLDIR, hiveWarehouseLocation + "/hrepl/");
    hconf.setVar(HiveConf.ConfVars.METASTOREURIS, "thrift://localhost:" + metaStorePort);
    hconf.setIntVar(HiveConf.ConfVars.METASTORETHRIFTCONNECTIONRETRIES, 3);
    hconf.set(HiveConf.ConfVars.PREEXECHOOKS.varname, "");
    hconf.set(HiveConf.ConfVars.POSTEXECHOOKS.varname, "");
    hconf.set(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY.varname, "false");
    System.setProperty(HiveConf.ConfVars.PREEXECHOOKS.varname, " ");
    System.setProperty(HiveConf.ConfVars.POSTEXECHOOKS.varname, " ");

    Path testPath = new Path(hiveWarehouseLocation);
    FileSystem fs = FileSystem.get(testPath.toUri(), hconf);
    fs.mkdirs(testPath);

    driver = new Driver(hconf);
    SessionState.start(new CliSessionState(hconf));
    client = new HiveMetaStoreClient(hconf);
  }

  private int next = 0;

  private void advanceDumpDir() {
    next++;
    ReplicationSemanticAnalyzer.injectNextDumpDirForTest(String.valueOf(next));
  }

  private ArrayList<String> lastResults;

  private String row0Result(int colNum, boolean reuse) throws IOException {
    if (!reuse) {
      lastResults = new ArrayList<>();
      try {
        driver.getResults(lastResults);
      } catch (CommandNeedRetryException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    }
    // Split around the 'tab' character
    return (lastResults.get(0).split("\\t"))[colNum];
  }

  WareHouse run(String command) throws Throwable {
    CommandProcessorResponse ret = driver.run(command);
    if (ret.getException() != null) {
      throw ret.getException();
    }
    return this;
  }

  Tuple dump(String dbName, String lastReplicationId) throws Throwable {
    advanceDumpDir();
    String dumpCommand =
        "REPL DUMP " + dbName + (lastReplicationId == null ? "" : " FROM " + lastReplicationId);
    run(dumpCommand);
    String dumpLocation = row0Result(0, false);
    String lastDumpId = row0Result(1, true);
    return new Tuple(dumpLocation, lastDumpId);
  }

  WareHouse load(String replicatedDbName, String dumpLocation) throws Throwable {
    run("EXPLAIN REPL LOAD " + replicatedDbName + " FROM '" + dumpLocation + "'");
    printOutput();
    run("REPL LOAD " + replicatedDbName + " FROM '" + dumpLocation + "'");
    return this;
  }

  WareHouse verify(String data) throws IOException {
    verifyResults(new String[] { data });
    return this;
  }

  /**
   * All the results that are read from the hive output will not preserve
   * case sensitivity and will all be in lower case, hence we will check against
   * only lower case data values.
   * Unless for Null Values it actually returns in UpperCase and hence explicitly lowering case
   * before assert.
   */
  private void verifyResults(String[] data) throws IOException {
    List<String> results = getOutput();
    TestReplicationScenariosAcrossInstances.LOG.info("Expecting {}", data);
    TestReplicationScenariosAcrossInstances.LOG.info("Got {}", results);
    assertEquals(data.length, results.size());
    for (int i = 0; i < data.length; i++) {
      assertEquals(data[i].toLowerCase(), results.get(i).toLowerCase());
    }
  }

  List<String> getOutput() throws IOException {
    List<String> results = new ArrayList<>();
    try {
      driver.getResults(results);
    } catch (CommandNeedRetryException e) {
      TestReplicationScenariosAcrossInstances.LOG.warn(e.getMessage(), e);
      throw new RuntimeException(e);
    }
    return results;
  }

  private void printOutput() throws IOException {
    for (String s : getOutput()) {
      TestReplicationScenariosAcrossInstances.LOG.info(s);
    }
  }

  static class Tuple {
    final String dumpLocation;
    final String lastReplicationId;

    Tuple(String dumpLocation, String lastReplicationId) {
      this.dumpLocation = dumpLocation;
      this.lastReplicationId = lastReplicationId;
    }
  }
}
