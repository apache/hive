/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.llap.security;

import java.io.IOException;
import java.util.Map;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.common.UgiFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.llap.LlapUtil;
import org.apache.hadoop.hive.shims.HadoopShims;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** No Java application is complete until it has a FactoryFactory. */
public class LlapUgiFactoryFactory {
  private static final Logger LOG = LoggerFactory.getLogger(LlapUgiFactoryFactory.class);

  private static final HadoopShims SHIMS = ShimLoader.getHadoopShims();

  /**
   * This class implements abstract logic for maintaining a single ugi for a specific user in a query.
   * Subclasses implement createNewUgiInternal for creating a new ugi object when needed.
   */
  private static abstract class AbstractLlapUgiFactory implements UgiFactory {
    Map<String, UserGroupInformation> ugis = new HashMap<>();

    /**
     * Creates an ugi for tasks in the same query and merges the credentials.
     * This is valid to be done once per query: no vertex-level ugi and credentials are needed, both of them
     * are the same within the same query.
     * Regarding vertex user: LlapTaskCommunicator has a single "user" field,
     * which is passed into the SignableVertexSpec.
     * Regarding credentials: LlapTaskCommunicator creates SubmitWorkRequestProto instances,
     * into which dag-level credentials are passed.
     * The most performant way would be to use a single UGI for the same user in the daemon, but that's not possible,
     * because the credentials can theoretically change across queries.
     */
    @Override
    public UserGroupInformation createUgi(String queryIdentifier, String user, Credentials credentials)
        throws IOException {
      // non-sync fast path for 99% of the calls in case of many tasks
      if (ugis.containsKey(queryIdentifier)) {
        UserGroupInformation ugi = ugis.get(queryIdentifier);
        LOG.debug("Ugi ({}) already exists for queryIdentifier '{}'", ugi, queryIdentifier);
        return ugi;
      }
      synchronized (this) {
        // double-check and return if previous thread already created ugi for this query
        if (ugis.containsKey(queryIdentifier)) {
          return ugis.get(queryIdentifier);
        }
        UserGroupInformation ugi = createNewUgiInternal(queryIdentifier, user, credentials);
        ugis.put(queryIdentifier, ugi);
        return ugi;
      }
    }

    @Override
    public void closeFileSystemsForQuery(String queryIdentifier) {
      LOG.debug("Closing all FileSystems for queryIdentifier '{}'", queryIdentifier);
      try {
        FileSystem.closeAllForUGI(ugis.get(queryIdentifier));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      ugis.remove(queryIdentifier);
    }

    protected abstract UserGroupInformation createNewUgiInternal(String queryIdentifier, String user,
        Credentials credentials) throws IOException;
  }

  private static class KerberosUgiFactory extends AbstractLlapUgiFactory implements UgiFactory {
    private final UserGroupInformation baseUgi;

    public KerberosUgiFactory(String keytab, String principal) throws IOException {
      baseUgi = LlapUtil.loginWithKerberos(principal, keytab);
    }

    @Override
    protected UserGroupInformation createNewUgiInternal(String queryIdentifier, String user, Credentials credentials)
        throws IOException {
      // Make sure the UGI is current.
      baseUgi.checkTGTAndReloginFromKeytab();
      // TODO: the only reason this is done this way is because we want unique Subject-s so that
      //       the FS.get gives different FS objects to different fragments.
      // TODO: could we log in from ticket cache instead? no good method on UGI right now
      UserGroupInformation ugi = SHIMS.cloneUgi(baseUgi);
      ugi.addCredentials(credentials);
      LOG.info("Created ugi in KerberosUgiFactory {} for queryIdentifier '{}', current ugis #: {}", ugi,
          queryIdentifier, ugis.size());
      return ugi;
    }
  }

  private static class NoopUgiFactory extends AbstractLlapUgiFactory implements UgiFactory {
    @Override
    protected UserGroupInformation createNewUgiInternal(String queryIdentifier, String user, Credentials credentials)
        throws IOException {
      // create clone in order to have a unique subject (== unique ugi) per query,
      // otherwise closeFileSystemsForQuery will close a FileSystem used by another query
      UserGroupInformation ugi = SHIMS.cloneUgi(UserGroupInformation.createRemoteUser(user));
      ugi.addCredentials(credentials);
      LOG.info("Created ugi in NoopUgiFactory {} for queryIdentifier '{}', current ugis #: {}", ugi, queryIdentifier,
          ugis.size());
      return ugi;
    }
  }

  public static UgiFactory createFsUgiFactory(Configuration conf) throws IOException {
    String fsKeytab = HiveConf.getVar(conf, ConfVars.LLAP_FS_KERBEROS_KEYTAB_FILE),
        fsPrincipal = HiveConf.getVar(conf, ConfVars.LLAP_FS_KERBEROS_PRINCIPAL);
    boolean hasFsKeytab = fsKeytab != null && !fsKeytab.isEmpty(),
        hasFsPrincipal = fsPrincipal != null && !fsPrincipal.isEmpty();
    if (hasFsKeytab != hasFsPrincipal) {
      throw new IOException("Inconsistent FS keytab settings " + fsKeytab + "; " + fsPrincipal);
    }
    return hasFsKeytab ? new KerberosUgiFactory(fsKeytab, fsPrincipal) : new NoopUgiFactory();
  }
}
