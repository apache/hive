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
package org.apache.hive.jdbc;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.security.UserGroupInformation;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.AbstractWaitStrategy;
import org.testcontainers.utility.DockerImageName;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class ITHiveServer2 extends ITAbstractContainer {
  protected File workDir;
  private final MiniJdbcKdc miniKdc;
  protected GenericContainer<?> container;
  protected final String imageName = "apache/hive:4.0.1";
  private int[] realmPorts;

  public ITHiveServer2() {
    this.miniKdc = null;
  }

  public ITHiveServer2(File workDir) throws Exception {
    this.workDir = workDir;
    this.miniKdc = new MiniJdbcKdc(this.workDir);
    String principal = MiniJdbcKdc.HIVE_TEST_USER_1;
    Configuration configuration = new Configuration();
    configuration.set("hadoop.security.authentication", "kerberos");
    UserGroupInformation.setConfiguration(configuration);
    UserGroupInformation.loginUserFromKeytab(principal, miniKdc.getKeyTabFile(principal));
    assertEquals(principal, UserGroupInformation.getLoginUser().getShortUserName());
  }

  protected Map<String, String> prepareEnvArgs() {
    Map<String, String> envArgs = new HashMap<>();
    envArgs.put("SERVICE_NAME", "hiveserver2");
    envArgs.put("HIVE_SERVER2_TRANSPORT_MODE", "all");
    List<String> properties = new ArrayList<>();
    properties.add("hive.server2.authentication=KERBEROS");
    properties.add("hive.server2.authentication.kerberos.principal=hive/_HOST@" + miniKdc.getRealm());
    properties.add("hive.server2.authentication.kerberos.keytab=/opt/hive/conf/hive.keytab");
    StringBuilder builder = new StringBuilder();
    for (String prop : properties) {
      builder.append("-D").append(prop).append(" ");
    }
    envArgs.put("SERVICE_OPTS", builder.toString());
    return envArgs;
  }

  private Map<String, String> prepareFiles() {
    Map<String, String> boundFiles = new HashMap<>();
    boundFiles.put(createNewKrbConf().getPath(), "/etc/krb5.conf");
    boundFiles.put(miniKdc.getKeyTabFile(miniKdc.getServicePrincipalForUser("hive")), "/opt/hive/conf/hive.keytab");
    boundFiles.put(ITHiveServer2.class.getClassLoader().getResource("core-site.xml").getPath(),
        "/opt/hive/conf/core-site.xml");
    // java.io.IOException: Can't get Master Kerberos principal for use as renewer if the yarn-site is not present
    boundFiles.put(ITHiveServer2.class.getClassLoader().getResource("yarn-site.xml").getPath(),
        "/opt/hive/conf/yarn-site.xml");
    return boundFiles;
  }

  @Override
  public void start() {
    container = new GenericContainer<>(DockerImageName.parse(imageName))
        .withEnv(prepareEnvArgs())
        .waitingFor(new AbstractWaitStrategy() {
          @Override
          protected void waitUntilReady() {
            long timeout = TimeUnit.MINUTES.toMillis(15);
            long start;
            do {
              start = System.currentTimeMillis();
              try (Connection conn = DriverManager.getConnection(getBaseJdbcUrl())) {
                break;
              } catch (Exception e) {
                try {
                  Thread.sleep(10 * 1000);
                } catch (InterruptedException ex) {
                  break;
                }
              }
            } while ((timeout += start - System.currentTimeMillis()) > 0);
          }
        });
    beforeStart(container);
    container.start();
  }

  protected void beforeStart(GenericContainer<?> container) {
    Map<String, String> boundFiles = prepareFiles();
    for (Map.Entry<String, String> entry : boundFiles.entrySet()) {
      container.withFileSystemBind(entry.getKey(), entry.getValue(), BindMode.READ_ONLY);
    }
    container
        .withCreateContainerCmdModifier(it -> it.withHostName(MiniJdbcKdc.HOST))
        .withExposedPorts(10000, 10001);
    if (realmPorts != null) {
      org.testcontainers.Testcontainers.exposeHostPorts(realmPorts);
    }
  }

  private File createNewKrbConf() {
    File krb5 = miniKdc.getKrb5conf();
    File newKrb5 = new File(workDir, krb5.getName() + "_new");
    try (BufferedReader reader = new BufferedReader(new FileReader(krb5));
         FileWriter writer = new FileWriter(newKrb5, false)) {
      String line;
      List<Integer> hostPorts = new ArrayList<>();
      String localhost = "localhost:";
      while ((line = reader.readLine()) != null) {
        if (line.contains(localhost)) {
          hostPorts.add(Integer.valueOf(line.split(localhost)[1]));
          line = line.replace("localhost", "host.testcontainers.internal");
        }
        writer.write(line);
        writer.write(System.lineSeparator());
      }
      this.realmPorts = new int[hostPorts.size()];
      for (int i = 0; i < hostPorts.size(); i++) {
        realmPorts[i] = hostPorts.get(i);
      }
      writer.flush();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return newKrb5;
  }

  @Override
  public void stop() throws Exception {
    try {
      miniKdc.stop();
    } finally {
      if (container != null) {
        container.stop();
      }
    }
  }

  @Override
  protected String getHttpJdbcUrl() {
    return "jdbc:hive2://" + MiniJdbcKdc.HOST + ":" + container.getMappedPort(10001) +
        "/;principal=" + miniKdc.getServicePrincipalForUser("hive") + "@" + miniKdc.getRealm() +
        ";transportMode=http;httpPath=cliservice";
  }

  @Override
  protected String getBaseJdbcUrl() {
    return "jdbc:hive2://" + MiniJdbcKdc.HOST + ":" + container.getMappedPort(10000) + "/;principal=" +
        miniKdc.getServicePrincipalForUser("hive") + "@" + miniKdc.getRealm();
  }

  public static class MiniJdbcKdc {
    protected static String HOST = "test-standalone-jdbc-kerberos";
    public static String HIVE_SERVICE_PRINCIPAL = "hive";
    public static String YARN_SERVICE_PRINCIPAL = "yarn";
    public static String HIVE_TEST_USER_1 = "user1";
    public static String HIVE_TEST_USER_2 = "user2";
    public static String HIVE_TEST_SUPER_USER = "superuser";
    private static int keyTabCounter = 1;
    private final MiniKdc miniKdc;
    private final Properties kdcConf = MiniKdc.createConf();
    private final Map<String, String> userPrincipals =
        new HashMap<String, String>();
    private final File workDir;

    public MiniJdbcKdc(File workDir) throws Exception {
      this.workDir = workDir;
      /**
       *  Hadoop security classes read the default realm via static initialization,
       *  before miniKdc is initialized. Hence we set the realm via a test configuration
       *  and propagate that to miniKdc.
       */
      assertNotNull("java.security.krb5.conf is needed for hadoop security",
          System.getProperty("java.security.krb5.conf"));
      System.clearProperty("java.security.krb5.conf");

      miniKdc = new MiniKdc(kdcConf, new File(workDir, "miniKdc"));
      miniKdc.start();

      // create default users
      addUserPrincipal(getServicePrincipalForUser(HIVE_SERVICE_PRINCIPAL));
      addUserPrincipal(getServicePrincipalForUser(YARN_SERVICE_PRINCIPAL));
      addUserPrincipal(HIVE_TEST_USER_1);
      addUserPrincipal(HIVE_TEST_USER_2);
      addUserPrincipal(HIVE_TEST_SUPER_USER);
    }

    public void addUserPrincipal(String principal) throws Exception {
      File keytab = new File(workDir, "miniKdc" + keyTabCounter++ + ".keytab");
      miniKdc.createPrincipal(keytab, principal);
      userPrincipals.put(principal, keytab.getPath());
    }

    public String getServicePrincipalForUser(String shortUserName) {
      return shortUserName + "/" + HOST;
    }

    public String getKeyTabFile(String principalName) {
      return userPrincipals.get(principalName);
    }

    public File getKrb5conf() {
      return miniKdc.getKrb5conf();
    }

    public String getRealm() {
      return miniKdc.getRealm();
    }

    public void stop() {
      miniKdc.stop();
    }
  }
}
