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

import java.lang.annotation.Annotation;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.llap.daemon.LlapDaemonProtocolBlockingPB;
import org.apache.hadoop.hive.llap.daemon.LlapManagementProtocolBlockingPB;
import org.apache.hadoop.security.KerberosInfo;
import org.apache.hadoop.security.SecurityInfo;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.security.token.TokenInfo;
import org.apache.hadoop.security.token.TokenSelector;

public class LlapServerSecurityInfo extends SecurityInfo {
  private static final Log LOG = LogFactory.getLog(LlapServerSecurityInfo.class);

  @Override
  public KerberosInfo getKerberosInfo(Class<?> protocol, Configuration conf) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Trying to get KerberosInfo for " + protocol);
    }
    if (!LlapDaemonProtocolBlockingPB.class.isAssignableFrom(protocol)
        && !LlapManagementProtocolBlockingPB.class.isAssignableFrom(protocol)) return null;
    return new KerberosInfo() {
      @Override
      public Class<? extends Annotation> annotationType() {
        return null;
      }

      @Override
      public String serverPrincipal() {
        return HiveConf.ConfVars.LLAP_KERBEROS_PRINCIPAL.varname;
      }

      @Override
      public String clientPrincipal() {
        return null;
      }
    };
  }

  @Override
  public TokenInfo getTokenInfo(Class<?> protocol, Configuration conf) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Trying to get TokenInfo for " + protocol);
    }
    // Tokens cannot be used for the management protocol (for now).
    if (!LlapDaemonProtocolBlockingPB.class.isAssignableFrom(protocol)) return null;
    return new TokenInfo() {
      @Override
      public Class<? extends Annotation> annotationType() {
        return null;
      }

      @Override
      public Class<? extends TokenSelector<? extends TokenIdentifier>> value() {
        return LlapTokenSelector.class;
      }
    };
  }
}
