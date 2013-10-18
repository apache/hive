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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hive.hcatalog.templeton;

import java.io.File;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.thrift.TException;

/**
 * Helper class to run jobs using Kerberos security.  Always safe to
 * use these methods, it's a noop if security is not enabled.
 */
public class SecureProxySupport {
  private Path tokenPath;
  public static final String HCAT_SERVICE = "hcat";
  private final boolean isEnabled;
  private String user;

  public SecureProxySupport() {
    isEnabled = UserGroupInformation.isSecurityEnabled();
  }

  private static final Log LOG = LogFactory.getLog(SecureProxySupport.class);

  /**
   * The file where we store the auth token
   */
  public Path getTokenPath() {
    return (tokenPath);
  }

  /**
   * The token to pass to hcat.
   */
  public String getHcatServiceStr() {
    return (HCAT_SERVICE);
  }

  /**
   * Create the delegation token.
   */
  public Path open(String user, Configuration conf)
    throws IOException, InterruptedException {
    close();
    if (isEnabled) {
      this.user = user;
      File t = File.createTempFile("templeton", null);
      tokenPath = new Path(t.toURI());
      Token fsToken = getFSDelegationToken(user, conf);
      String hcatTokenStr;
      try {
        hcatTokenStr = buildHcatDelegationToken(user);
      } catch (Exception e) {
        throw new IOException(e);
      }
      Token<?> msToken = new Token();
      msToken.decodeFromUrlString(hcatTokenStr);
      msToken.setService(new Text(HCAT_SERVICE));
      writeProxyDelegationTokens(fsToken, msToken, conf, user, tokenPath);

    }
    return tokenPath;
  }

  /**
   * Cleanup
   */
  public void close() {
    if (tokenPath != null) {
      new File(tokenPath.toUri()).delete();
      tokenPath = null;
    }
  }

  /**
   * Add Hadoop env variables.
   */
  public void addEnv(Map<String, String> env) {
    if (isEnabled) {
      env.put(UserGroupInformation.HADOOP_TOKEN_FILE_LOCATION,
        getTokenPath().toUri().getPath());
    }
  }

  /**
   * Add hcat args.
   */
  public void addArgs(List<String> args) {
    if (isEnabled) {
      args.add("-D");
      args.add("hive.metastore.token.signature=" + getHcatServiceStr());
      args.add("-D");
      args.add("proxy.user.name=" + user);
    }
  }

  class TokenWrapper {
    Token<?> token;
  }

  private Token<?> getFSDelegationToken(String user,
                      final Configuration conf)
    throws IOException, InterruptedException {
    LOG.info("user: " + user + " loginUser: " + UserGroupInformation.getLoginUser().getUserName());
    final UserGroupInformation ugi = UgiFactory.getUgi(user);

    final TokenWrapper twrapper = new TokenWrapper();
    ugi.doAs(new PrivilegedExceptionAction<Object>() {
      public Object run() throws IOException {
        FileSystem fs = FileSystem.get(conf);
        twrapper.token = fs.getDelegationToken(ugi.getShortUserName());
        return null;
      }
    });
    return twrapper.token;

  }

  private void writeProxyDelegationTokens(final Token<?> fsToken,
                      final Token<?> msToken,
                      final Configuration conf,
                      String user,
                      final Path tokenPath)
    throws IOException, InterruptedException {


    LOG.info("user: " + user + " loginUser: " + UserGroupInformation.getLoginUser().getUserName());
    final UserGroupInformation ugi = UgiFactory.getUgi(user);


    ugi.doAs(new PrivilegedExceptionAction<Object>() {
      public Object run() throws IOException {
        Credentials cred = new Credentials();
        cred.addToken(fsToken.getService(), fsToken);
        cred.addToken(msToken.getService(), msToken);
        cred.writeTokenStorageFile(tokenPath, conf);
        return null;
      }
    });

  }

  private String buildHcatDelegationToken(String user)
    throws IOException, InterruptedException, MetaException, TException {
    HiveConf c = new HiveConf();
    final HiveMetaStoreClient client = new HiveMetaStoreClient(c);
    LOG.info("user: " + user + " loginUser: " + UserGroupInformation.getLoginUser().getUserName());
    final TokenWrapper twrapper = new TokenWrapper();
    final UserGroupInformation ugi = UgiFactory.getUgi(user);
    String s = ugi.doAs(new PrivilegedExceptionAction<String>() {
      public String run()
        throws IOException, MetaException, TException {
        String u = ugi.getUserName();
        return client.getDelegationToken(u);
      }
    });
    return s;
  }
}
