/**
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

package org.apache.hive.jdbc.miniHS2;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.ql.WindowsPathUtil;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.shims.HadoopShims.MiniDFSShim;
import org.apache.hadoop.hive.shims.HadoopShims.MiniMrShim;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hive.service.Service;
import org.apache.hive.service.cli.CLIServiceClient;
import org.apache.hive.service.cli.SessionHandle;
import org.apache.hive.service.cli.thrift.ThriftBinaryCLIService;
import org.apache.hive.service.cli.thrift.ThriftCLIServiceClient;
import org.apache.hive.service.cli.thrift.ThriftHttpCLIService;
import org.apache.hive.service.server.HiveServer2;

import com.google.common.io.Files;

public class MiniHS2 extends AbstractHiveService {
  public static final String HS2_BINARY_MODE = "binary";
  public static final String HS2_HTTP_MODE = "http";
  private static final String driverName = "org.apache.hive.jdbc.HiveDriver";
  private static final FsPermission FULL_PERM = new FsPermission((short)00777);
  private static final FsPermission WRITE_ALL_PERM = new FsPermission((short)00733);
  private HiveServer2 hiveServer2 = null;
  private final File baseDir;
  private final Path baseDfsDir;
  private static final AtomicLong hs2Counter = new AtomicLong();
  private MiniMrShim mr;
  private MiniDFSShim dfs;
  private FileSystem localFS;
  private boolean useMiniKdc = false;
  private final String serverPrincipal;
  private final boolean isMetastoreRemote;
  private MiniClusterType miniClusterType = MiniClusterType.DFS_ONLY;

  public enum MiniClusterType {
    MR,
    TEZ,
    DFS_ONLY;
  }

  public static class Builder {
    private HiveConf hiveConf = new HiveConf();
    private MiniClusterType miniClusterType = MiniClusterType.DFS_ONLY;
    private boolean useMiniKdc = false;
    private String serverPrincipal;
    private String serverKeytab;
    private boolean isHTTPTransMode = false;
    private boolean isMetastoreRemote;

    public Builder() {
    }

    public Builder withMiniMR() {
      this.miniClusterType = MiniClusterType.MR;
      return this;
    }

    public Builder withMiniKdc(String serverPrincipal, String serverKeytab) {
      this.useMiniKdc = true;
      this.serverPrincipal = serverPrincipal;
      this.serverKeytab = serverKeytab;
      return this;
    }

    public Builder withRemoteMetastore() {
      this.isMetastoreRemote = true;
      return this;
    }

    public Builder withConf(HiveConf hiveConf) {
      this.hiveConf = hiveConf;
      return this;
    }

    /**
     * Start HS2 with HTTP transport mode, default is binary mode
     * @return this Builder
     */
    public Builder withHTTPTransport(){
      this.isHTTPTransMode = true;
      return this;
    }


    public MiniHS2 build() throws Exception {
      if (miniClusterType == MiniClusterType.MR && useMiniKdc) {
        throw new IOException("Can't create secure miniMr ... yet");
      }
      if (isHTTPTransMode) {
        hiveConf.setVar(ConfVars.HIVE_SERVER2_TRANSPORT_MODE, HS2_HTTP_MODE);
      } else {
        hiveConf.setVar(ConfVars.HIVE_SERVER2_TRANSPORT_MODE, HS2_BINARY_MODE);
      }
      return new MiniHS2(hiveConf, miniClusterType, useMiniKdc, serverPrincipal, serverKeytab,
          isMetastoreRemote);
    }
  }

  public MiniMrShim getMr() {
    return mr;
  }

  public void setMr(MiniMrShim mr) {
    this.mr = mr;
  }

  public MiniDFSShim getDfs() {
    return dfs;
  }

  public void setDfs(MiniDFSShim dfs) {
    this.dfs = dfs;
  }

  public FileSystem getLocalFS() {
    return localFS;
  }

  public MiniClusterType getMiniClusterType() {
    return miniClusterType;
  }

  public void setMiniClusterType(MiniClusterType miniClusterType) {
    this.miniClusterType = miniClusterType;
  }

  public boolean isUseMiniKdc() {
    return useMiniKdc;
  }

  private MiniHS2(HiveConf hiveConf, MiniClusterType miniClusterType, boolean useMiniKdc,
      String serverPrincipal, String serverKeytab, boolean isMetastoreRemote) throws Exception {
    super(hiveConf, "localhost", MetaStoreUtils.findFreePort(), MetaStoreUtils.findFreePort());
    this.miniClusterType = miniClusterType;
    this.useMiniKdc = useMiniKdc;
    this.serverPrincipal = serverPrincipal;
    this.isMetastoreRemote = isMetastoreRemote;
    baseDir = Files.createTempDir();
    localFS = FileSystem.getLocal(hiveConf);
    FileSystem fs;

    if (miniClusterType != MiniClusterType.DFS_ONLY) {
      // Initialize dfs
      dfs = ShimLoader.getHadoopShims().getMiniDfs(hiveConf, 4, true, null);
      fs = dfs.getFileSystem();
      String uriString = WindowsPathUtil.getHdfsUriString(fs.getUri().toString());

      // Initialize the execution engine based on cluster type
      switch (miniClusterType) {
      case TEZ:
        mr = ShimLoader.getHadoopShims().getMiniTezCluster(hiveConf, 4, uriString, 1);
        break;
      case MR:
        mr = ShimLoader.getHadoopShims().getMiniMrCluster(hiveConf, 4, uriString, 1);
        break;
      default:
        throw new IllegalArgumentException("Unsupported cluster type " + mr);
      }
      // store the config in system properties
      mr.setupConfiguration(getHiveConf());
      baseDfsDir =  new Path(new Path(fs.getUri()), "/base");
    } else {
      // This is DFS only mode, just initialize the dfs root directory.
      fs = FileSystem.getLocal(hiveConf);
      baseDfsDir = new Path("file://"+ baseDir.toURI().getPath());
    }
    if (useMiniKdc) {
      hiveConf.setVar(ConfVars.HIVE_SERVER2_KERBEROS_PRINCIPAL, serverPrincipal);
      hiveConf.setVar(ConfVars.HIVE_SERVER2_KERBEROS_KEYTAB, serverKeytab);
      hiveConf.setVar(ConfVars.HIVE_SERVER2_AUTHENTICATION, "KERBEROS");
    }
    String metaStoreURL =  "jdbc:derby:" + baseDir.getAbsolutePath() + File.separator + "test_metastore-" +
        hs2Counter.incrementAndGet() + ";create=true";

    fs.mkdirs(baseDfsDir);
    Path wareHouseDir = new Path(baseDfsDir, "warehouse");
    // Create warehouse with 777, so that user impersonation has no issues.
    FileSystem.mkdirs(fs, wareHouseDir, FULL_PERM);

    fs.mkdirs(wareHouseDir);
    setWareHouseDir(wareHouseDir.toString());
    System.setProperty(HiveConf.ConfVars.METASTORECONNECTURLKEY.varname, metaStoreURL);
    hiveConf.setVar(HiveConf.ConfVars.METASTORECONNECTURLKEY, metaStoreURL);
    // reassign a new port, just in case if one of the MR services grabbed the last one
    setBinaryPort(MetaStoreUtils.findFreePort());
    hiveConf.setVar(ConfVars.HIVE_SERVER2_THRIFT_BIND_HOST, getHost());
    hiveConf.setIntVar(ConfVars.HIVE_SERVER2_THRIFT_PORT, getBinaryPort());
    hiveConf.setIntVar(ConfVars.HIVE_SERVER2_THRIFT_HTTP_PORT, getHttpPort());

    Path scratchDir = new Path(baseDfsDir, "scratch");
    // Create root scratchdir with write all, so that user impersonation has no issues.
    Utilities.createDirsWithPermission(hiveConf, scratchDir, WRITE_ALL_PERM, true);
    System.setProperty(HiveConf.ConfVars.SCRATCHDIR.varname, scratchDir.toString());
    hiveConf.setVar(ConfVars.SCRATCHDIR, scratchDir.toString());

    String localScratchDir = baseDir.getPath() + File.separator + "scratch";
    System.setProperty(HiveConf.ConfVars.LOCALSCRATCHDIR.varname, localScratchDir);
    hiveConf.setVar(ConfVars.LOCALSCRATCHDIR, localScratchDir);
  }

  public MiniHS2(HiveConf hiveConf) throws Exception {
    this(hiveConf, MiniClusterType.DFS_ONLY);
  }

  public MiniHS2(HiveConf hiveConf, MiniClusterType clusterType) throws Exception {
    this(hiveConf, clusterType, false, null, null, false);
  }

  public void start(Map<String, String> confOverlay) throws Exception {
    if (isMetastoreRemote) {
      int metaStorePort = MetaStoreUtils.findFreePort();
      getHiveConf().setVar(ConfVars.METASTOREURIS, "thrift://localhost:" + metaStorePort);
      MetaStoreUtils.startMetaStore(metaStorePort,
      ShimLoader.getHadoopThriftAuthBridge(), getHiveConf());
    }

    hiveServer2 = new HiveServer2();
    // Set confOverlay parameters
    for (Map.Entry<String, String> entry : confOverlay.entrySet()) {
      setConfProperty(entry.getKey(), entry.getValue());
    }
    hiveServer2.init(getHiveConf());
    hiveServer2.start();
    waitForStartup();
    setStarted(true);
  }

  public void stop() {
    verifyStarted();
    // Currently there is no way to stop the MetaStore service. It will be stopped when the
    // test JVM exits. This is how other tests are also using MetaStore server.

    hiveServer2.stop();
    setStarted(false);
    try {
      if (mr != null) {
        mr.shutdown();
        mr = null;
      }
      if (dfs != null) {
        dfs.shutdown();
        dfs = null;
      }
    } catch (IOException e) {
      // Ignore errors cleaning up miniMR
    }
    FileUtils.deleteQuietly(baseDir);
  }

  public CLIServiceClient getServiceClient() {
    verifyStarted();
    return getServiceClientInternal();
  }

  public CLIServiceClient getServiceClientInternal() {
    for (Service service : hiveServer2.getServices()) {
      if (service instanceof ThriftBinaryCLIService) {
        return new ThriftCLIServiceClient((ThriftBinaryCLIService) service);
      }
      if (service instanceof ThriftHttpCLIService) {
        return new ThriftCLIServiceClient((ThriftHttpCLIService) service);
      }
    }
    throw new IllegalStateException("HiveServer2 not running Thrift service");
  }

  /**
   * return connection URL for this server instance
   * @return
   * @throws Exception
   */
  public String getJdbcURL() throws Exception {
    return getJdbcURL("default");
  }

  /**
   * return connection URL for this server instance
   * @param dbName - DB name to be included in the URL
   * @return
   * @throws Exception
   */
  public String getJdbcURL(String dbName) throws Exception {
    return getJdbcURL(dbName, "");
  }

  /**
   * return connection URL for this server instance
   * @param dbName - DB name to be included in the URL
   * @param sessionConfExt - Addional string to be appended to sessionConf part of url
   * @return
   * @throws Exception
   */
  public String getJdbcURL(String dbName, String sessionConfExt) throws Exception {
    return getJdbcURL(dbName, sessionConfExt, "");
  }

  /**
   * return connection URL for this server instance
   * @param dbName - DB name to be included in the URL
   * @param sessionConfExt - Addional string to be appended to sessionConf part of url
   * @param hiveConfExt - Additional string to be appended to HiveConf part of url (excluding the ?)
   * @return
   * @throws Exception
   */
  public String getJdbcURL(String dbName, String sessionConfExt, String hiveConfExt)
      throws Exception {
    sessionConfExt = (sessionConfExt == null ? "" : sessionConfExt);
    hiveConfExt = (hiveConfExt == null ? "" : hiveConfExt);
    // Strip the leading ";" if provided
    // (this is the assumption with which we're going to start configuring sessionConfExt)
    if (sessionConfExt.startsWith(";")) {
      sessionConfExt = sessionConfExt.substring(1);
    }
    if (isUseMiniKdc()) {
      sessionConfExt = "principal=" + serverPrincipal + ";" + sessionConfExt;
    }
    if (isHttpTransportMode()) {
      sessionConfExt = "transportMode=http;httpPath=cliservice" + ";" + sessionConfExt;
    }
    String baseJdbcURL = getBaseJdbcURL();
    baseJdbcURL = baseJdbcURL + dbName;
    if (!sessionConfExt.isEmpty()) {
      baseJdbcURL = baseJdbcURL + ";" + sessionConfExt;
    }
    if ((hiveConfExt != null) && (!hiveConfExt.trim().isEmpty())) {
      baseJdbcURL = baseJdbcURL + "?" + hiveConfExt;
    }
    return baseJdbcURL;
  }

  /**
   * Build base JDBC URL
   * @return
   */
  public String getBaseJdbcURL() {
    if(isHttpTransportMode()) {
      return "jdbc:hive2://" + getHost() + ":" + getHttpPort() + "/";
    }
    else {
      return "jdbc:hive2://" + getHost() + ":" + getBinaryPort() + "/";
    }
  }

  private boolean isHttpTransportMode() {
    String transportMode = getConfProperty(ConfVars.HIVE_SERVER2_TRANSPORT_MODE.varname);
    return transportMode != null && (transportMode.equalsIgnoreCase(HS2_HTTP_MODE));
  }

  public static String getJdbcDriverName() {
    return driverName;
  }

  public MiniMrShim getMR() {
    return mr;
  }

  public MiniDFSShim getDFS() {
    return dfs;
  }

  private void waitForStartup() throws Exception {
    int waitTime = 0;
    long startupTimeout = 1000L * 1000L;
    CLIServiceClient hs2Client = getServiceClientInternal();
    SessionHandle sessionHandle = null;
    do {
      Thread.sleep(500L);
      waitTime += 500L;
      if (waitTime > startupTimeout) {
        throw new TimeoutException("Couldn't access new HiveServer2: " + getJdbcURL());
      }
      try {
        Map <String, String> sessionConf = new HashMap<String, String>();
        /**
        if (isUseMiniKdc()) {
          getMiniKdc().loginUser(getMiniKdc().getDefaultUserPrincipal());
          sessionConf.put("principal", serverPrincipal);
        }
         */
        sessionHandle = hs2Client.openSession("foo", "bar", sessionConf);
      } catch (Exception e) {
        // service not started yet
        continue;
      }
      hs2Client.closeSession(sessionHandle);
      break;
    } while (true);
  }
}
