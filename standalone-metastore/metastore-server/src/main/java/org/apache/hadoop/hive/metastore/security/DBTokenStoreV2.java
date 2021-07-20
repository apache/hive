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

package org.apache.hadoop.hive.metastore.security;

import java.io.IOException;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSecretManager.DelegationTokenInformation;
import org.apache.hadoop.security.token.delegation.MetastoreDelegationTokenSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DBTokenStoreV2 extends DBTokenStore {
    private static final Logger LOG = LoggerFactory.getLogger(DBTokenStoreV2.class.getName());
    private static HadoopThriftAuthBridge.Server.ServerMode serverMode = null;

    @Override
    public void init(Object handler, HadoopThriftAuthBridge.Server.ServerMode smode) throws TokenStoreException {
        super.init(handler, smode);
        serverMode = smode;
    }

    @Override
    public boolean addToken(DelegationTokenIdentifier tokenIdentifier,
                            DelegationTokenInformation token) throws TokenStoreException {

        boolean result;
        try {
            String identifier = TokenStoreDelegationTokenSecretManager.encodeWritable(tokenIdentifier);
            int maxDate = (int) (tokenIdentifier.getMaxDate()/1000);
            String tokenStr = Base64.encodeBase64URLSafeString(
                    MetastoreDelegationTokenSupport.encodeDelegationTokenInformation(token));
            int renewDate = (int) (token.getRenewDate()/1000);
            if (serverMode.equals(HadoopThriftAuthBridge.Server.ServerMode.HIVESERVER2)) {
                result = (Boolean)invokeOnTokenStore("addToken", new Object[] {identifier, tokenStr},
                        String.class, String.class);
            } else {
                result = (Boolean)invokeOnTokenStore("addTokenV2", new Object[] {identifier, tokenStr, maxDate, renewDate},
                        String.class, String.class, int.class, int.class);
            }

            LOG.debug("DBTokenStoreV2 addToken: tokenIdentifier = {}, added = {}", tokenIdentifier, result);
            return result;
        } catch (IOException e) {
            throw new TokenStoreException(e);
        }
    }

    @Override
    public DelegationTokenInformation getToken(DelegationTokenIdentifier tokenIdentifier)
            throws TokenStoreException {
        String methodName;
        if (serverMode.equals(HadoopThriftAuthBridge.Server.ServerMode.HIVESERVER2)) {
            methodName = "getToken";
        } else {
            methodName = "getTokenV2";
        }
        try {
            String tokenStr = (String)invokeOnTokenStore(methodName, new Object[] {
                    TokenStoreDelegationTokenSecretManager.encodeWritable(tokenIdentifier)}, String.class);
            DelegationTokenInformation result = null;
            if (StringUtils.isNotEmpty(tokenStr)) {
                result = MetastoreDelegationTokenSupport.decodeDelegationTokenInformation(Base64.decodeBase64(tokenStr));
            }

            LOG.debug("DBTokenStoreV2 getToken: tokenIdentifier = {}, result = {}", tokenIdentifier, result);
            return result;
        } catch (IOException e) {
            throw new TokenStoreException(e);
        }
    }

    @Override
    public boolean removeToken(DelegationTokenIdentifier tokenIdentifier) throws TokenStoreException{
        String methodName;
        if (serverMode.equals(HadoopThriftAuthBridge.Server.ServerMode.HIVESERVER2)) {
            methodName = "removeToken";
        } else {
            methodName = "removeTokenV2";
        }
        try {
            boolean result = (Boolean)invokeOnTokenStore(methodName, new Object[] {
                    TokenStoreDelegationTokenSecretManager.encodeWritable(tokenIdentifier)}, String.class);
            LOG.debug("DBTokenStoreV2 removeToken: tokenIdentifier = {}, removed = {}", tokenIdentifier, result);
            return result;
        } catch (IOException e) {
            throw new TokenStoreException(e);
        }
    }

    @Override
    public boolean isTokenStoreExpirySupported() { return true; }

    @Override
    public void removeExpiredTokens() {
        int result = (Integer)invokeOnTokenStore("removeExpiredTokens", new Object[0]);
        LOG.debug("DBTokenStoreV2 removeExpiredTokens, removed = {}", result);
    }
}
