/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hive.jdbc;

import java.io.File;
import java.io.FileWriter;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.api.WMFullResourcePlan;
import org.apache.hadoop.hive.metastore.api.WMPool;
import org.apache.hadoop.hive.metastore.api.WMPoolTrigger;
import org.apache.hadoop.hive.metastore.api.WMResourcePlan;
import org.apache.hadoop.hive.ql.exec.tez.WorkloadManager;
import org.apache.hadoop.hive.ql.wm.Trigger;
import org.apache.hive.jdbc.miniHS2.MiniHS2;
import org.apache.hive.jdbc.miniHS2.MiniHS2.MiniClusterType;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.rules.TestName;

import com.google.common.collect.Lists;

@Ignore("Disabled in HIVE-20074 temporary as it is unstable, Will re-enable in HIVE-20075.")
public class TestTriggersWorkloadManager extends TestTriggersTezSessionPoolManager {
  @Rule
  public TestName testName = new TestName();

  @Override
  public String getTestName() {
    return getClass().getSimpleName() + "#" + testName.getMethodName();
  }

  @BeforeClass
  public static void beforeTest() throws Exception {
    Class.forName(MiniHS2.getJdbcDriverName());

    String confDir = "../../data/conf/llap/";
    HiveConf.setHiveSiteLocation(new URL("file://" + new File(confDir).toURI().getPath() + "/hive-site.xml"));
    conf = new HiveConf();
    conf.setVar(ConfVars.HIVE_AUTHENTICATOR_MANAGER, "org.apache.hadoop.hive.ql.security.SessionStateUserAuthenticator");
    java.nio.file.Path confPath = File.createTempFile("hive", "test").toPath();
    conf.writeXml(new FileWriter(confPath.toFile()));
    HiveConf.setHiveSiteLocation(new URL("file://" + confPath.toString()));

    System.out.println("Setting hive-site: " + HiveConf.getHiveSiteLocation());

    conf = new HiveConf();
    conf.setBoolVar(ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
    conf.setBoolVar(ConfVars.HIVE_SERVER2_ENABLE_DOAS, false);
    conf.setTimeVar(ConfVars.HIVE_TRIGGER_VALIDATION_INTERVAL, 100, TimeUnit.MILLISECONDS);
    conf.setVar(ConfVars.HIVE_SERVER2_TEZ_INTERACTIVE_QUEUE, "default");
    conf.setBoolean("hive.test.workload.management", true);
    conf.setBoolVar(ConfVars.TEZ_EXEC_SUMMARY, true);
    conf.setBoolVar(ConfVars.HIVE_STRICT_CHECKS_CARTESIAN, false);
    // don't want cache hits from llap io for testing filesystem bytes read counters
    conf.setVar(ConfVars.LLAP_IO_MEMORY_MODE, "none");

    conf.addResource(new URL("file://" + new File(confDir).toURI().getPath()
      + "/tez-site.xml"));

    miniHS2 = new MiniHS2(conf, MiniClusterType.LLAP);
    dataFileDir = conf.get("test.data.files").replace('\\', '/').replace("c:", "");
    kvDataFilePath = new Path(dataFileDir, "kv1.txt");

    Map<String, String> confOverlay = new HashMap<>();
    miniHS2.start(confOverlay);
    miniHS2.getDFS().getFileSystem().mkdirs(new Path("/apps_staging_dir/anonymous"));
  }

  @Override
  protected void setupTriggers(final List<Trigger> triggers) throws Exception {
    WorkloadManager wm = WorkloadManager.getInstance();
    WMPool pool = new WMPool("rp", "llap");
    pool.setAllocFraction(1.0f);
    pool.setQueryParallelism(1);
    WMFullResourcePlan rp = new WMFullResourcePlan(
        new WMResourcePlan("rp"), Lists.newArrayList(pool));
    rp.getPlan().setDefaultPoolPath("llap");
    for (Trigger trigger : triggers) {
      rp.addToTriggers(wmTriggerFromTrigger(trigger));
      rp.addToPoolTriggers(new WMPoolTrigger("llap", trigger.getName()));
    }
    wm.updateResourcePlanAsync(rp).get(10, TimeUnit.SECONDS);
  }
}
