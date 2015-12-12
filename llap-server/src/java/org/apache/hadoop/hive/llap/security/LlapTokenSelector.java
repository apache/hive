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

package org.apache.hadoop.hive.llap.security;

import java.util.Collection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.security.token.TokenSelector;

public class LlapTokenSelector implements TokenSelector<LlapTokenIdentifier> {
  private static final Log LOG = LogFactory.getLog(LlapTokenSelector.class);

  @Override
  public Token<LlapTokenIdentifier> selectToken(Text service,
      Collection<Token<? extends TokenIdentifier>> tokens) {
    if (service == null) return null;
    if (LOG.isDebugEnabled()) {
      LOG.debug("Looking for a token with service " + service);
    }
    for (Token<? extends TokenIdentifier> token : tokens) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Token = " + token.getKind() + "; service = " + token.getService());
      }
      if (LlapTokenIdentifier.KIND_NAME.equals(token.getKind())
          && service.equals(token.getService())) {
        @SuppressWarnings("unchecked")
        Token<LlapTokenIdentifier> result = (Token<LlapTokenIdentifier>)token;
        return result;
      }
    }
    return null;
  }
}
