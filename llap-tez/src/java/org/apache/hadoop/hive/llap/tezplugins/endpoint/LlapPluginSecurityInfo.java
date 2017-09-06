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

package org.apache.hadoop.hive.llap.tezplugins.endpoint;

import java.lang.annotation.Annotation;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.llap.protocol.LlapPluginProtocolPB;
import org.apache.hadoop.hive.llap.protocol.LlapProtocolBlockingPB;
import org.apache.hadoop.hive.llap.protocol.LlapManagementProtocolPB;
import org.apache.hadoop.security.KerberosInfo;
import org.apache.hadoop.security.SecurityInfo;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.security.token.TokenInfo;
import org.apache.hadoop.security.token.TokenSelector;
import org.apache.tez.runtime.common.security.JobTokenSelector;

public class LlapPluginSecurityInfo extends SecurityInfo {
  private static final Log LOG = LogFactory.getLog(LlapPluginSecurityInfo.class);

  @Override
  public KerberosInfo getKerberosInfo(Class<?> protocol, Configuration conf) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Trying to get KerberosInfo for " + protocol);
    }
    return null;
  }

  @Override
  public TokenInfo getTokenInfo(Class<?> protocol, Configuration conf) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Trying to get TokenInfo for " + protocol);
    }
    if (!LlapPluginProtocolPB.class.isAssignableFrom(protocol)) return null;
    return new TokenInfo() {
      @Override
      public Class<? extends Annotation> annotationType() {
        return null;
      }

      @Override
      public Class<? extends TokenSelector<? extends TokenIdentifier>> value() {
        return JobTokenSelector.class;
      }
    };
  }
}
