/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.hive.llap;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.PolicyProvider;
import org.apache.hadoop.security.token.SecretManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.BlockingService;

import javax.annotation.Nullable;

public class LlapUtil {
  private static final Logger LOG = LoggerFactory.getLogger(LlapUtil.class);

  public static String getDaemonLocalDirString(Configuration conf, String workDirsEnvString) {
    String localDirList = HiveConf.getVar(conf, ConfVars.LLAP_DAEMON_WORK_DIRS);
    if (localDirList != null && !localDirList.isEmpty()) {
      LOG.info("Local dirs from Configuration: {}", localDirList);
      if (!localDirList.equalsIgnoreCase("useYarnEnvDirs") &&
          !StringUtils.isBlank(localDirList)) {
        LOG.info("Using local dirs from Configuration");
        return localDirList;
      }
    }
    // Fallback to picking up the value from environment.
    if (StringUtils.isNotBlank(workDirsEnvString)) {
      LOG.info("Using local dirs from environment: {}", workDirsEnvString);
      return workDirsEnvString;
    } else {
      throw new RuntimeException(
          "Cannot determined local dirs from specified configuration and env. ValueFromConf=" +
              localDirList + ", ValueFromEnv=" + workDirsEnvString);
    }
  }

  /**
   * Login using kerberos. But does not change the current logged in user.
   *
   * @param principal  - kerberos principal
   * @param keytabFile - keytab file
   * @return UGI
   * @throws IOException - if keytab file cannot be found
   */
  public static UserGroupInformation loginWithKerberos(
    String principal, String keytabFile) throws IOException {
    if (!UserGroupInformation.isSecurityEnabled()) {
      return null;
    }
    if (principal == null || principal.isEmpty() || keytabFile == null || keytabFile.isEmpty()) {
      throw new RuntimeException("Kerberos principal and/or keytab are null or empty");
    }
    final String serverPrincipal = SecurityUtil.getServerPrincipal(principal, "0.0.0.0");
    LOG.info("Logging in as " + serverPrincipal + " via " + keytabFile);
    return UserGroupInformation.loginUserFromKeytabAndReturnUGI(serverPrincipal, keytabFile);
  }

  /**
   * Login using kerberos and also updates the current logged in user
   *
   * @param principal  - kerberos principal
   * @param keytabFile - keytab file
   * @throws IOException - if keytab file cannot be found
   */
  public static void loginWithKerberosAndUpdateCurrentUser(String principal, String keytabFile) throws IOException {
    if (!UserGroupInformation.isSecurityEnabled()) {
      return;
    }
    if (principal == null || principal.isEmpty() || keytabFile == null || keytabFile.isEmpty()) {
      throw new RuntimeException("Kerberos principal and/or keytab is null or empty");
    }
    final String serverPrincipal = SecurityUtil.getServerPrincipal(principal, "0.0.0.0");
    LOG.info("Logging in as " + serverPrincipal + " via " + keytabFile + " and updating current logged in user");
    UserGroupInformation.loginUserFromKeytab(serverPrincipal, keytabFile);
  }

  private final static Pattern hostsRe = Pattern.compile("[^A-Za-z0-9_-]");
  public static String generateClusterName(Configuration conf) {
    String hosts = HiveConf.getTrimmedVar(conf, ConfVars.LLAP_DAEMON_SERVICE_HOSTS);
    return hostsRe.matcher(hosts.startsWith("@") ? hosts.substring(1) : hosts).replaceAll("_");
  }

  public static String getUserNameFromPrincipal(String principal) {
    // Based on SecurityUtil.
    if (principal == null) return null;
    String[] components = principal.split("[/@]");
    return (components == null || components.length != 3) ? principal : components[0];
  }

  public static List<StatisticsData> getStatisticsForScheme(final String scheme,
      final List<StatisticsData> stats) {
    List<StatisticsData> result = new ArrayList<>();
    if (stats != null && scheme != null) {
      for (StatisticsData s : stats) {
        if (s.getScheme().equalsIgnoreCase(scheme)) {
          result.add(s);
        }
      }
    }
    return result;
  }

  public static Map<String, FileSystem.Statistics> getCombinedFileSystemStatistics() {
    final List<FileSystem.Statistics> allStats = FileSystem.getAllStatistics();
    final Map<String, FileSystem.Statistics> result = new HashMap<>();
    for (FileSystem.Statistics statistics : allStats) {
      final String scheme = statistics.getScheme();
      if (result.containsKey(scheme)) {
        FileSystem.Statistics existing = result.get(scheme);
        FileSystem.Statistics combined = combineFileSystemStatistics(existing, statistics);
        result.put(scheme, combined);
      } else {
        result.put(scheme, statistics);
      }
    }
    return result;
  }

  private static FileSystem.Statistics combineFileSystemStatistics(final FileSystem.Statistics s1,
      final FileSystem.Statistics s2) {
    FileSystem.Statistics result = new FileSystem.Statistics(s1);
    result.incrementReadOps(s2.getReadOps());
    result.incrementLargeReadOps(s2.getLargeReadOps());
    result.incrementWriteOps(s2.getWriteOps());
    result.incrementBytesRead(s2.getBytesRead());
    result.incrementBytesWritten(s2.getBytesWritten());
    return result;
  }

  public static List<StatisticsData> cloneThreadLocalFileSystemStatistics() {
    List<StatisticsData> result = new ArrayList<>();
    // thread local filesystem stats is private and cannot be cloned. So make a copy to new class
    for (FileSystem.Statistics statistics : FileSystem.getAllStatistics()) {
      result.add(new StatisticsData(statistics.getScheme(), statistics.getThreadStatistics()));
    }
    return result;
  }

  public static class StatisticsData {
    long bytesRead;
    long bytesWritten;
    int readOps;
    int largeReadOps;
    int writeOps;
    String scheme;

    public StatisticsData(String scheme, FileSystem.Statistics.StatisticsData fsStats) {
      this.scheme = scheme;
      this.bytesRead = fsStats.getBytesRead();
      this.bytesWritten = fsStats.getBytesWritten();
      this.readOps = fsStats.getReadOps();
      this.largeReadOps = fsStats.getLargeReadOps();
      this.writeOps = fsStats.getWriteOps();
    }

    public long getBytesRead() {
      return bytesRead;
    }

    public long getBytesWritten() {
      return bytesWritten;
    }

    public int getReadOps() {
      return readOps;
    }

    public int getLargeReadOps() {
      return largeReadOps;
    }

    public int getWriteOps() {
      return writeOps;
    }

    public String getScheme() {
      return scheme;
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append(" scheme: ").append(scheme);
      sb.append(" bytesRead: ").append(bytesRead);
      sb.append(" bytesWritten: ").append(bytesWritten);
      sb.append(" readOps: ").append(readOps);
      sb.append(" largeReadOps: ").append(largeReadOps);
      sb.append(" writeOps: ").append(writeOps);
      return sb.toString();
    }
  }

  public static String getAmHostNameFromAddress(InetSocketAddress address, Configuration conf) {
    if (!HiveConf.getBoolVar(conf, HiveConf.ConfVars.LLAP_DAEMON_AM_USE_FQDN)) {
      return address.getHostName();
    }
    InetAddress ia = address.getAddress();
    // getCanonicalHostName would either return FQDN, or an IP.
    return (ia == null) ? address.getHostName() : ia.getCanonicalHostName();
  }

  public static String humanReadableByteCount(long bytes) {
    int unit = 1024;
    if (bytes < unit) {
      return bytes + "B";
    }
    int exp = (int) (Math.log(bytes) / Math.log(unit));
    String suffix = "KMGTPE".charAt(exp-1) + "";
    return String.format("%.2f%sB", bytes / Math.pow(unit, exp), suffix);
  }

  public static RPC.Server createRpcServer(Class<?> pbProtocol, InetSocketAddress addr,
      Configuration conf, int numHandlers, BlockingService blockingService,
      SecretManager<?> secretManager, PolicyProvider provider, ConfVars... aclVars)
          throws IOException {
    Configuration serverConf = conf;
    boolean isSecurityEnabled = conf.getBoolean(
        CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION, false);
    if (isSecurityEnabled) {
      // Enforce Hive defaults.
      for (ConfVars acl : aclVars) {
        if (conf.get(acl.varname) != null) continue; // Some value is set.
        if (serverConf == conf) {
          serverConf = new Configuration(conf);
        }
        serverConf.set(acl.varname, HiveConf.getVar(serverConf, acl)); // Set the default.
      }
    }
    RPC.setProtocolEngine(serverConf, pbProtocol, ProtobufRpcEngine.class);
    RPC.Builder builder = new RPC.Builder(serverConf)
        .setProtocol(pbProtocol)
        .setInstance(blockingService)
        .setBindAddress(addr.getHostName())
        .setPort(addr.getPort())
        .setNumHandlers(numHandlers);
    if (secretManager != null) {
      builder = builder.setSecretManager(secretManager);
    }
    RPC.Server server = builder.build();
    if (isSecurityEnabled) {
      server.refreshServiceAcl(serverConf, provider);
    }
    return server;
  }

  public static RPC.Server startProtocolServer(int srvPort, int numHandlers,
      AtomicReference<InetSocketAddress> bindAddress, Configuration conf,
      BlockingService impl, Class<?> protocolClass, SecretManager<?> secretManager,
      PolicyProvider provider, ConfVars... aclVars) {
    InetSocketAddress addr = new InetSocketAddress(srvPort);
    RPC.Server server;
    try {
      server = createRpcServer(protocolClass, addr, conf,
          numHandlers, impl, secretManager, provider, aclVars);
      server.start();
    } catch (IOException e) {
      LOG.error("Failed to run RPC Server on port: " + srvPort, e);
      throw new RuntimeException(e);
    }

    InetSocketAddress serverBindAddress = NetUtils.getConnectAddress(server);
    InetSocketAddress bindAddressVal = NetUtils.createSocketAddrForHost(
        serverBindAddress.getAddress().getCanonicalHostName(),
        serverBindAddress.getPort());
    if (bindAddress != null) {
      bindAddress.set(bindAddressVal);
    }
    LOG.info("Instantiated " + protocolClass.getSimpleName() + " at " + bindAddressVal);
    return server;
  }

  // Copied from AcidUtils so we don't have to put the code using this into ql.
  // TODO: Ideally, AcidUtils class and various constants should be in common.
  private static final String BASE_PREFIX = "base_", DELTA_PREFIX = "delta_",
      DELETE_DELTA_PREFIX = "delete_delta_", BUCKET_PREFIX = "bucket_",
      DATABASE_PATH_SUFFIX = ".db", UNION_SUDBIR_PREFIX = "HIVE_UNION_SUBDIR_";

  @Deprecated
  public static String getDbAndTableNameForMetrics(Path path, boolean includeParts) {
    String[] parts = path.toUri().getPath().toString().split(Path.SEPARATOR);
    int dbIx = -1;
    // Try to find the default db postfix; don't check two last components - at least there
    // should be a table and file (we could also try to throw away partition/bucket/acid stuff).
    for (int i = 0; i < parts.length - 2; ++i) {
      if (!parts[i].endsWith(DATABASE_PATH_SUFFIX)) continue;
      if (dbIx >= 0) {
        dbIx = -1; // Let's not guess which one is correct.
        break;
      }
      dbIx = i;
    }
    if (dbIx >= 0) {
      String dbAndTable = parts[dbIx].substring(
          0, parts[dbIx].length() - 3) + "." + parts[dbIx + 1];
      if (!includeParts) return dbAndTable;
      for (int i = dbIx + 2; i < parts.length; ++i) {
        if (!parts[i].contains("=")) break;
        dbAndTable += "/" + parts[i];
      }
      return dbAndTable;
    }

    // Just go from the back and throw away everything we think is wrong; skip last item, the file.
    boolean isInPartFields = false;
    for (int i = parts.length - 2; i >= 0; --i) {
      String p = parts[i];
      boolean isPartField = p.contains("=");
      if ((isInPartFields && !isPartField) || (!isPartField && !isSomeHiveDir(p))) {
        dbIx = i - 1; // Assume this is the table we are at now.
        break;
      }
      isInPartFields = isPartField;
    }
    // If we found something before we ran out of components, use it.
    if (dbIx >= 0) {
      String dbName = parts[dbIx];
      if (dbName.endsWith(DATABASE_PATH_SUFFIX)) {
        dbName = dbName.substring(0, dbName.length() - 3);
      }
      String dbAndTable = dbName + "." + parts[dbIx + 1];
      if (!includeParts) return dbAndTable;
      for (int i = dbIx + 2; i < parts.length; ++i) {
        if (!parts[i].contains("=")) break;
        dbAndTable += "/" + parts[i];
      }
      return dbAndTable;
    }
    return "unknown";
  }

  private static boolean isSomeHiveDir(String p) {
    return p.startsWith(BASE_PREFIX) || p.startsWith(DELTA_PREFIX) || p.startsWith(BUCKET_PREFIX)
        || p.startsWith(UNION_SUDBIR_PREFIX) || p.startsWith(DELETE_DELTA_PREFIX);
  }


  @Nullable public static ThreadMXBean initThreadMxBean() {
    ThreadMXBean mxBean = ManagementFactory.getThreadMXBean();
    if (mxBean != null) {
      if (!mxBean.isCurrentThreadCpuTimeSupported()) {
        LOG.warn("Thread CPU monitoring is not supported");
        return null;
      } else if (!mxBean.isThreadCpuTimeEnabled()) {
        LOG.warn("Thread CPU monitoring is not enabled");
        return null;
      }
    }
    return mxBean;
  }

  /**
   * transform a byte of crendetials to a hadoop Credentials object.
   * @param binaryCredentials credentials in byte format as they would
   *                          usually be when received from protobuffers
   * @return a hadoop Credentials object
   */
  public static Credentials credentialsFromByteArray(byte[] binaryCredentials)
      throws IOException  {
    Credentials credentials = new Credentials();
    DataInputBuffer dib = new DataInputBuffer();
    dib.reset(binaryCredentials, binaryCredentials.length);
    credentials.readTokenStorageStream(dib);
    return credentials;
  }

  /**
   * @return returns the value of LLAP_EXTERNAL_CLIENT_CLOUD_DEPLOYMENT_SETUP_ENABLED
   * @param conf
   */
  public static boolean isCloudDeployment(Configuration conf) {
    return HiveConf.getBoolVar(conf, ConfVars.LLAP_EXTERNAL_CLIENT_CLOUD_DEPLOYMENT_SETUP_ENABLED, false);
  }

  /**
   * @return returns the value of PUBLIC_HOSTNAME from either environment variable or system properties
   */
  public static String getPublicHostname() {
    String publicHostname = System.getenv("PUBLIC_HOSTNAME");
    if (publicHostname == null) {
      publicHostname = System.getProperty("PUBLIC_HOSTNAME");
    }
    return publicHostname;
  }

}
