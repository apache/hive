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

package org.apache.hadoop.hive.ql.security;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaStore.HMSHandler;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.ql.metadata.AuthorizationException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.security.authorization.HiveMetastoreAuthorizationProvider;
import org.apache.hadoop.hive.ql.security.authorization.Privilege;

/**
 * Dummy implementation for use by unit tests. Tracks the context of calls made to
 * its authorize functions in {@link AuthCallContext}
 */
public class DummyHiveMetastoreAuthorizationProvider implements HiveMetastoreAuthorizationProvider {


  protected HiveAuthenticationProvider authenticator;

  public enum AuthCallContextType {
    USER,
    DB,
    TABLE,
    PARTITION,
    TABLE_AND_PARTITION,
    AUTHORIZATION
  };

  class AuthCallContext {

    public AuthCallContextType type;
    public List<Object> authObjects;
    public Privilege[] readRequiredPriv;
    public Privilege[] writeRequiredPriv;

    AuthCallContext(AuthCallContextType typeOfCall,
        Privilege[] readRequiredPriv, Privilege[] writeRequiredPriv) {
      this.type = typeOfCall;
      this.authObjects = new ArrayList<Object>();
      this.readRequiredPriv = readRequiredPriv;
      this.writeRequiredPriv = writeRequiredPriv;
    }
    AuthCallContext(AuthCallContextType typeOfCall, Object authObject,
        Privilege[] readRequiredPriv, Privilege[] writeRequiredPriv) {
      this(typeOfCall,readRequiredPriv,writeRequiredPriv);
      this.authObjects.add(authObject);
    }
    AuthCallContext(AuthCallContextType typeOfCall, List<? extends Object> authObjects,
        Privilege[] readRequiredPriv, Privilege[] writeRequiredPriv) {
      this(typeOfCall,readRequiredPriv,writeRequiredPriv);
      this.authObjects.addAll(authObjects);
    }
  }

  public static final List<AuthCallContext> authCalls = new ArrayList<AuthCallContext>();

  private Configuration conf;
  public static final Logger LOG = LoggerFactory.getLogger(
      DummyHiveMetastoreAuthorizationProvider.class);;

  @Override
  public Configuration getConf() {
    return this.conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
    try {
      init(conf);
    } catch (HiveException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public HiveAuthenticationProvider getAuthenticator() {
    return authenticator;
  }

  @Override
  public void setAuthenticator(HiveAuthenticationProvider authenticator) {
    this.authenticator = authenticator;
  }

  @Override
  public void init(Configuration conf) throws HiveException {
    debugLog("DHMAP.init");
  }

  @Override
  public void authorize(Privilege[] readRequiredPriv, Privilege[] writeRequiredPriv)
      throws HiveException, AuthorizationException {
    debugLog("DHMAP.authorize " +
      "read:" + debugPrivPrint(readRequiredPriv) +
      " , write:" + debugPrivPrint(writeRequiredPriv)
      );
    authCalls.add(new AuthCallContext(AuthCallContextType.USER,
        readRequiredPriv, writeRequiredPriv));
  }

  @Override
  public void authorize(Database db, Privilege[] readRequiredPriv, Privilege[] writeRequiredPriv)
      throws HiveException, AuthorizationException {
    debugLog("DHMAP.authorizedb " +
        "db:" + db.getName() +
        " , read:" + debugPrivPrint(readRequiredPriv) +
        " , write:" + debugPrivPrint(writeRequiredPriv)
        );
    authCalls.add(new AuthCallContext(AuthCallContextType.DB,
        db, readRequiredPriv, writeRequiredPriv));
  }

  @Override
  public void authorize(Table table, Privilege[] readRequiredPriv, Privilege[] writeRequiredPriv)
      throws HiveException, AuthorizationException {
    debugLog("DHMAP.authorizetbl " +
        "tbl:" + table.getCompleteName() +
        " , read:" + debugPrivPrint(readRequiredPriv) +
        " , write:" + debugPrivPrint(writeRequiredPriv)
        );
    authCalls.add(new AuthCallContext(AuthCallContextType.TABLE,
        table, readRequiredPriv, writeRequiredPriv));

  }

  @Override
  public void authorize(Partition part, Privilege[] readRequiredPriv, Privilege[] writeRequiredPriv)
      throws HiveException, AuthorizationException {
    debugLog("DHMAP.authorizepart " +
        "tbl:" + part.getTable().getCompleteName() +
        " , part: " + part.getName() +
        " , read:" + debugPrivPrint(readRequiredPriv) +
        " , write:" + debugPrivPrint(writeRequiredPriv)
        );
    authCalls.add(new AuthCallContext(AuthCallContextType.PARTITION,
        part, readRequiredPriv, writeRequiredPriv));

  }

  @Override
  public void authorize(Table table, Partition part, List<String> columns,
      Privilege[] readRequiredPriv, Privilege[] writeRequiredPriv) throws HiveException,
      AuthorizationException {
    debugLog("DHMAP.authorizecols " +
        "tbl:" + table.getCompleteName() +
        " , part: " + part.getName() +
        " . cols: " + columns.toString() +
        " , read:" + debugPrivPrint(readRequiredPriv) +
        " , write:" + debugPrivPrint(writeRequiredPriv)
        );
    List<Object> authObjects = new ArrayList<Object>();
    authObjects.add(table);
    authObjects.add(part);
    authCalls.add(new AuthCallContext(AuthCallContextType.TABLE_AND_PARTITION,
        authObjects, readRequiredPriv, writeRequiredPriv));

  }

  private void debugLog(String s) {
    LOG.debug(s);
  }

  private String debugPrivPrint(Privilege[] privileges) {
    StringBuffer sb = new StringBuffer();
    sb.append("Privileges{");
    if (privileges != null){
    for (Privilege p : privileges){
      sb.append(p.toString());
    }
    }else{
      sb.append("null");
    }
    sb.append("}");
    return sb.toString();
  }

  @Override
  public void setMetaStoreHandler(HMSHandler handler) {
    debugLog("DHMAP.setMetaStoreHandler");
  }

  @Override
  public void authorizeAuthorizationApiInvocation() throws HiveException, AuthorizationException {
    debugLog("DHMAP.authorizeauthapi");
    authCalls.add(new AuthCallContext(AuthCallContextType.AUTHORIZATION, null, null));
  }



}
