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
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LlapUtil {
  private static final Logger LOG = LoggerFactory.getLogger(LlapUtil.class);

  public static String getDaemonLocalDirList(Configuration conf) {
    String localDirList = HiveConf.getVar(conf, ConfVars.LLAP_DAEMON_WORK_DIRS);
    if (localDirList != null && !localDirList.isEmpty()) return localDirList;
    return conf.get("yarn.nodemanager.local-dirs");
  }

  public static UserGroupInformation loginWithKerberos(
      String principal, String keytabFile) throws IOException {
    if (!UserGroupInformation.isSecurityEnabled()) return null;
    if (principal.isEmpty() || keytabFile.isEmpty()) {
      throw new RuntimeException("Kerberos principal and/or keytab are empty");
    }
    LOG.info("Logging in as " + principal + " via " + keytabFile);
    return UserGroupInformation.loginUserFromKeytabAndReturnUGI(
        SecurityUtil.getServerPrincipal(principal, "0.0.0.0"), keytabFile);
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
}
