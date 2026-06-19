/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.anon;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.ColumnInternalFormat;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.anon.consts.AnonConst;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.TestHive;
import org.apache.hadoop.hive.ql.processors.CommandProcessorException;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.util.StringUtils;
import org.apache.tez.dag.api.TezConfiguration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DEFAULT_NAME_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVE_DDL_OUTPUT_FORMAT;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVE_JAR_DIRECTORY;
import static org.apache.hadoop.hive.ql.anon.TestUtils.WH_DIR;
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class BaseTest {

  protected final Logger LOG = LoggerFactory.getLogger(BaseTest.class);
  protected static Driver driver;
  protected static SessionState sessionState;
  protected static HiveConf conf;

  protected String tblName;
  protected Hive hive;

  @BeforeAll
  public void onetimeSetup() throws Exception {
    Path tmp = Paths.get("/tmp/hive-test");
    if (!Files.exists(tmp)) {
      Files.createDirectory(tmp);
    }
    System.setProperty(HIVE_JAR_DIRECTORY.varname, tmp.toString());
    conf = new HiveConf(Driver.class);
    conf.setVar(HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER, SQLStdHiveAuthorizerFactory.class.getCanonicalName());
    HiveConf.setBoolVar(conf, HiveConf.ConfVars.HIVE_AUTHORIZATION_ENABLED, false);
    conf.setBoolean(AnonConst.ANON_POLICY_ENFORCE_PRIVILEGES, false);
    conf.setVar(HiveConf.ConfVars.HIVE_EXECUTION_ENGINE, "tez");
    conf.setBoolean(TezConfiguration.TEZ_LOCAL_MODE, true);
    conf.set(FS_DEFAULT_NAME_KEY, FS_DEFAULT_NAME_DEFAULT);
    HiveConf.setBoolVar(conf, HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
    HiveConf.setBoolVar(conf, HiveConf.ConfVars.HIVE_CBO_ENABLED, false);
    HiveConf.setBoolVar(conf, HiveConf.ConfVars.HIVE_VECTORIZATION_ENABLED, false);
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.WAREHOUSE, WH_DIR);

    final String scratch = System.getProperty("dae.test.scratch");
    if (scratch != null && !scratch.isEmpty()) {
      conf.set("hadoop.tmp.dir", scratch + "/hadoop");
      conf.set("hive.exec.scratchdir", scratch + "/hive-scratch");
      conf.set("hive.exec.local.scratchdir", scratch + "/hive-local");
      conf.set("yarn.nodemanager.local-dirs", scratch + "/nm-local");
      conf.set("mapreduce.cluster.local.dir", scratch + "/mr-local");
      conf.set("tez.staging-dir", scratch + "/tez-staging");
    }

    sessionState = SessionState.start(conf);
    driver = new Driver(conf);
    try {
      hive = Hive.get(conf);
    } catch (Exception e) {
      System.err.println(StringUtils.stringifyException(e));
      System.err.println("Unable to initialize Hive Metastore using configuration: \n" + conf);
      throw e;
    }

    resetManagedErasurePolicies();
  }

  protected String[] managedErasurePolicies() {
    return new String[0];
  }

  protected void resetManagedErasurePolicies() {
    final String[] names = managedErasurePolicies();
    if (names == null || names.length == 0) {
      return;
    }
    final String url = conf.get("javax.jdo.option.ConnectionURL");
    if (url == null || !url.startsWith("jdbc:postgresql")) {
      return;
    }
    final String user = conf.get("javax.jdo.option.ConnectionUserName");
    final String pw = conf.get("javax.jdo.option.ConnectionPassword");

    final StringBuilder list = new StringBuilder();
    for (int i = 0; i < names.length; i++) {
      if (i > 0) {
        list.append(',');
      }
      list.append('\'').append(names[i].replace("'", "''")).append('\'');
    }
    final String pid = "(SELECT \"POLICY_ID\" FROM \"ERASURE_POLICIES\" WHERE \"POLICY_NAME\" IN (" + list + "))";
    final String ver = "(SELECT \"VERSION_ID\" FROM \"ERASURE_POLICY_VERSIONS\" WHERE \"POLICY_ID\" IN " + pid + ")";
    final String bnd = "(SELECT \"BINDING_ID\" FROM \"ERASURE_POLICY_BINDING_MEMBERS\" WHERE \"POLICY_ID\" IN " + pid + ")";
    final String[] stmts = {
        "DELETE FROM \"ERASURE_POLICY_LIFECYCLE_EVENTS\" WHERE \"VERSION_ID\" IN " + ver + " OR \"BINDING_ID\" IN " + bnd,
        "DELETE FROM \"ERASURE_POLICY_STATEMENTS\" WHERE \"VERSION_ID\" IN " + ver,
        "DELETE FROM \"ERASURE_RUN_AUDIT_IDENTITY\" WHERE \"RUN_ID\" IN (SELECT \"RUN_ID\" FROM \"ERASURE_RUN_AUDIT\" WHERE \"BINDING_ID\" IN " + bnd + ")",
        "DELETE FROM \"ERASURE_RUN_AUDIT\" WHERE \"BINDING_ID\" IN " + bnd,
        "DELETE FROM \"ERASURE_POLICY_BINDING_RESOLVED\" WHERE \"BINDING_ID\" IN " + bnd,
        "DELETE FROM \"ERASURE_POLICY_BINDING_MEMBERS\" WHERE \"POLICY_ID\" IN " + pid,
        "DELETE FROM \"ERASURE_POLICY_VERSIONS\" WHERE \"POLICY_ID\" IN " + pid,
        "DELETE FROM \"COL_POLICIES\" WHERE \"POLICY_ID\" IN " + pid,
        "UPDATE \"COLUMNS_V2\" SET \"POLICY_ID\"=NULL WHERE \"POLICY_ID\" IN " + pid,
        "DELETE FROM \"ERASURE_POLICIES\" WHERE \"POLICY_NAME\" IN (" + list + ")",
    };
    try (Connection c = DriverManager.getConnection(url, user, pw)) {
      c.setAutoCommit(false);
      try (Statement st = c.createStatement()) {
        for (final String s : stmts) {
          st.executeUpdate(s);
        }
        c.commit();
      } catch (SQLException e) {
        c.rollback();
        throw e;
      }
    } catch (SQLException e) {
      LOG.warn("resetManagedErasurePolicies: best-effort cleanup of {} failed (continuing): {}",
          Arrays.toString(names), e.getMessage());
    }
  }

  private static Hive setUpImpl(HiveConf hiveConf) throws Exception {
    hiveConf.setVar(HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER,
            "org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory");
    hiveConf.setFloat("fs.trash.checkpoint.interval", 30);
    hiveConf.setFloat("fs.trash.interval", 30);
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_IN_TEST, true);
    hiveConf.setBoolVar(HiveConf.ConfVars.FIRE_EVENTS_FOR_DML, true);
    MetastoreConf.setVar(hiveConf, MetastoreConf.ConfVars.EVENT_LISTENERS, TestHive.DummyFireInsertListener.class.getName());
    MetastoreConf.setBoolVar(hiveConf, MetastoreConf.ConfVars.HIVE_IN_TEST, true);
    SessionState.start(hiveConf);
    try {
      return Hive.get(hiveConf);
    } catch (Exception e) {
      System.err.println(StringUtils.stringifyException(e));
      System.err.println("Unable to initialize Hive Metastore using configuration: \n" + hiveConf);
      throw e;
    }
  }

  @AfterAll
  public void cleanUp() throws Exception {
    driver.close();
    driver.destroy();
  }

  protected List<Object> execute(String sql) throws CommandProcessorException {
    if (tblName != null && sql.contains("%s")) {
      sql = sql.replace("%s", tblName);
    }
    final CommandProcessorResponse response = driver.run(sql);
    LOG.info("schema {}", response.getSchema());

    final List<Object> lst = new ArrayList<>();
    try {
      if (driver.getResults(lst)) {
        for (Object o : lst) {
          LOG.info(o.toString());
        }
      }
      return lst;
    } catch (IOException ioe) {
      throw new CommandProcessorException(ioe);
    }
  }

  protected void truncate() throws CommandProcessorException {
    execute("truncate table " + tblName);
  }

}
