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

package org.apache.hadoop.hive.llap.security;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.llap.LlapUgiFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.llap.LlapUtil;
import org.apache.hadoop.hive.shims.HadoopShims;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.security.UserGroupInformation;

public class LlapUgiHelper {
  private static final HadoopShims SHIMS = ShimLoader.getHadoopShims();

  public static LlapUgiFactory createLlapUgiFactory(Configuration conf) throws IOException {
    String fsKeytab = HiveConf.getVar(conf, ConfVars.LLAP_FS_KERBEROS_KEYTAB_FILE),
        fsPrincipal = HiveConf.getVar(conf, ConfVars.LLAP_FS_KERBEROS_PRINCIPAL);
    boolean hasFsKeytab = fsKeytab != null && !fsKeytab.isEmpty(),
        hasFsPrincipal = fsPrincipal != null && !fsPrincipal.isEmpty();
    if (hasFsKeytab != hasFsPrincipal) {
      throw new IOException("Inconsistent FS keytab settings " + fsKeytab + "; " + fsPrincipal);
    }
    return hasFsKeytab ? new KerberosLlapUgiFactory(fsKeytab, fsPrincipal) : new NoopLlapUgiFactory();
  }

  private static class KerberosLlapUgiFactory implements LlapUgiFactory {
    private final UserGroupInformation baseUgi;

    public KerberosLlapUgiFactory(String keytab, String principal) throws IOException {
      baseUgi = LlapUtil.loginWithKerberos(principal, keytab);
    }

    @Override
    public UserGroupInformation createUgi(String user) throws IOException {
      // Make sure the UGI is current.
      baseUgi.checkTGTAndReloginFromKeytab();
      // TODO: the only reason this is done this way is because we want unique Subject-s so that
      //       the FS.get gives different FS objects to different fragments.
      // TODO: could we log in from ticket cache instead? no good method on UGI right now
      return SHIMS.cloneUgi(baseUgi);
    }
  }

  private static class NoopLlapUgiFactory implements LlapUgiFactory {
    @Override
    public UserGroupInformation createUgi(String user) throws IOException {
      // create clone in order to have a unique subject (== unique ugi) per query,
      // otherwise closeFileSystemsForQuery will close a FileSystem used by another query
      return SHIMS.cloneUgi(UserGroupInformation.createRemoteUser(user));
    }
  }
}
