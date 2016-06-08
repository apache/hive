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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.UgiFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.llap.LlapUtil;
import org.apache.hadoop.hive.shims.HadoopShims;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.security.UserGroupInformation;

/** No Java application is complete until it has a FactoryFactory. */
public class LlapUgiFactoryFactory {
  private static final HadoopShims SHIMS = ShimLoader.getHadoopShims();

  private static class KerberosUgiFactory implements UgiFactory {
    private final UserGroupInformation baseUgi;

    public KerberosUgiFactory(String keytab, String principal) throws IOException {
      baseUgi = LlapUtil.loginWithKerberos(principal, keytab);
    }

    @Override
    public UserGroupInformation createUgi() throws IOException {
      // Make sure the UGI is current.
      baseUgi.checkTGTAndReloginFromKeytab();
      // TODO: the only reason this is done this way is because we want unique Subject-s so that
      //       the FS.get gives different FS objects to different fragments.
      // TODO: could we log in from ticket cache instead? no good method on UGI right now.
      return SHIMS.cloneUgi(baseUgi);
    }
  }

  private static class NoopUgiFactory implements UgiFactory {
    @Override
    public UserGroupInformation createUgi() throws IOException {
      return null;
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
