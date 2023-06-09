/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.kafka;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorFactory;
import org.apache.hadoop.hive.ql.plan.FileSinkDesc;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.security.token.Token;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.file.Paths;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;

public class KafkaDagCredentialSupplierTest {
  private static final java.nio.file.Path KEYSTORE_DIR =
      Paths.get(System.getProperty("test.tmp.dir"), "kdc_root_dir" + UUID.randomUUID());
  private static final String HIVE_USER_NAME = "hive";
  private static final java.nio.file.Path HIVE_USER_KEYTAB = KEYSTORE_DIR.resolve(HIVE_USER_NAME + ".keytab");
  private static final String KAFKA_USER_NAME = "kafka";
  private static final java.nio.file.Path KAFKA_USER_KEYTAB = KEYSTORE_DIR.resolve(KAFKA_USER_NAME + ".keytab");
  private static final KafkaBrokerResource KAFKA_BROKER_RESOURCE =
      new KafkaBrokerResource().enableSASL(KAFKA_USER_NAME, KAFKA_USER_KEYTAB.toString());
  private static MiniKdc kdc = null;

  private static MiniKdc initKDC() {
    try {
      Properties conf = MiniKdc.createConf();
      MiniKdc kdc = new MiniKdc(conf, KEYSTORE_DIR.toFile());
      kdc.start();
      kdc.createPrincipal(HIVE_USER_KEYTAB.toFile(), HIVE_USER_NAME + "/localhost");
      kdc.createPrincipal(KAFKA_USER_KEYTAB.toFile(), KAFKA_USER_NAME + "/localhost");
      return kdc;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @BeforeClass
  public static void startCluster() throws Throwable {
    kdc = initKDC();
    KAFKA_BROKER_RESOURCE.before();
  }

  @AfterClass
  public static void stopCluster() {
    KAFKA_BROKER_RESOURCE.after();
    kdc.stop();
  }

  @Test
  public void testObtainTokenNotNull() {
    HiveConf conf = new HiveConf();
    conf.setVar(HiveConf.ConfVars.HIVE_SERVER2_AUTHENTICATION, "KERBEROS");
    conf.setVar(HiveConf.ConfVars.HIVE_SERVER2_KERBEROS_PRINCIPAL, HIVE_USER_NAME + "/localhost");
    conf.setVar(HiveConf.ConfVars.HIVE_SERVER2_KERBEROS_KEYTAB, HIVE_USER_KEYTAB.toString());

    TableDesc fileSinkDesc = createKafkaDesc();
    MapWork work = createFileSinkWork(fileSinkDesc);
    KafkaDagCredentialSupplier supplier = new KafkaDagCredentialSupplier();
    Token<?> t = supplier.obtainToken(work, Collections.singleton(fileSinkDesc), conf);
    Assert.assertNotNull(t);
    Assert.assertEquals(new Text("kafka"), t.getService());
  }

  private static MapWork createFileSinkWork(TableDesc tableDesc) {
    MapWork work = new MapWork();
    Path fakePath = new Path("fake", "path");
    Operator<FileSinkDesc> op =
        OperatorFactory.get(new CompilationOpContext(), new FileSinkDesc(fakePath, tableDesc, true));
    work.addMapWork(fakePath, "fakeAlias", op, null);
    return work;
  }

  private static TableDesc createKafkaDesc() {
    Properties props = new Properties();
    props.setProperty("name", "kafka_table_fake");
    props.setProperty("kafka.bootstrap.servers", KafkaBrokerResource.BROKER_SASL_PORT);
    return new TableDesc(KafkaInputFormat.class, KafkaOutputFormat.class, props);
  }
}
