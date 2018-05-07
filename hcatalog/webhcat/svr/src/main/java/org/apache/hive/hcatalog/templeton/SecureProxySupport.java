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
import java.net.URI;
import java.net.URISyntaxException;
import java.security.PrivilegedExceptionAction;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hive.hcatalog.common.HCatUtil;
import org.apache.thrift.TException;

/**
 * Helper class to run jobs using Kerberos security.  Always safe to
 * use these methods, it's a no-op if security is not enabled.
 */
public class SecureProxySupport {
  private Path tokenPath;
  public static final String HCAT_SERVICE = "hcat";
  private final boolean isEnabled;
  private String user;

  public SecureProxySupport() {
    isEnabled = UserGroupInformation.isSecurityEnabled();
  }

  private static final Logger LOG = LoggerFactory.getLogger(SecureProxySupport.class);

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
      Token[] fsToken = getFSDelegationToken(user, conf);
      String hcatTokenStr;
      try {
        hcatTokenStr = buildHcatDelegationToken(user);
      } catch (Exception e) {
        throw new IOException(e);
      }
      if(hcatTokenStr == null) {
        LOG.error("open(" + user + ") token=null");
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
      String checksumStr = tokenPath.getParent() + File.separator + "." + tokenPath.getName() + ".crc";
      File checksumFile = null;
      try {
        checksumFile = new File(new URI(checksumStr));
        if (checksumFile.exists()) {
          checksumFile.delete();
        }
      } catch (URISyntaxException e) {
        LOG.error("Failed to delete token crc file.", e);
      }
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
      args.add(HiveConf.ConfVars.METASTORE_TOKEN_SIGNATURE + "=" + getHcatServiceStr());
      args.add("-D");
      args.add("proxy.user.name=" + user);
    }
  }

  private static class TokenWrapper {
    Token<?>[] tokens = new Token<?>[0];
  }

  private Token<?>[] getFSDelegationToken(String user,
                      final Configuration conf)
    throws IOException, InterruptedException {
    LOG.info("user: " + user + " loginUser: " + UserGroupInformation.getLoginUser().getUserName());
    final UserGroupInformation ugi = UgiFactory.getUgi(user);

    final TokenWrapper twrapper = new TokenWrapper();
    ugi.doAs(new PrivilegedExceptionAction<Object>() {
      public Object run() throws IOException, URISyntaxException {
        Credentials creds = new Credentials();
        //get Tokens for default FS.  Not all FSs support delegation tokens, e.g. WASB
        collectTokens(FileSystem.get(conf), twrapper, creds, ugi.getShortUserName());
        //get tokens for all other known FSs since Hive tables may result in different ones
        //passing "creds" prevents duplicate tokens from being added
        Collection<String> URIs = conf.getStringCollection("mapreduce.job.hdfs-servers");
        for(String uri : URIs) {
          LOG.debug("Getting tokens for " + uri);
          collectTokens(FileSystem.get(new URI(uri), conf), twrapper, creds, ugi.getShortUserName());
        }
        return null;
      }
    });
    FileSystem.closeAllForUGI(ugi);
    return twrapper.tokens;
  }
  private static void collectTokens(FileSystem fs, TokenWrapper twrapper, Credentials creds, String userName) throws IOException {
    Token[] tokens = fs.addDelegationTokens(userName, creds);
    if(tokens != null && tokens.length > 0) {
      twrapper.tokens = ArrayUtils.addAll(twrapper.tokens, tokens);
    }
  }
  /**
   * @param fsTokens not null
   */
  private void writeProxyDelegationTokens(final Token<?> fsTokens[],
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
        for(Token<?> fsToken : fsTokens) {
          cred.addToken(fsToken.getService(), fsToken);
        }
        cred.addToken(msToken.getService(), msToken);
        cred.writeTokenStorageFile(tokenPath, conf);
        return null;
      }
    });
    FileSystem.closeAllForUGI(ugi);

  }

  private String buildHcatDelegationToken(String user)
    throws IOException, InterruptedException, TException {
    final HiveConf c = new HiveConf();
    final IMetaStoreClient client = HCatUtil.getHiveMetastoreClient(c);
    LOG.info("user: " + user + " loginUser: " + UserGroupInformation.getLoginUser().getUserName());
    final UserGroupInformation ugi = UgiFactory.getUgi(user);
    String s = ugi.doAs(new PrivilegedExceptionAction<String>() {
      public String run()
        throws IOException, MetaException, TException {
        String u = ugi.getUserName();
        return client.getDelegationToken(c.getUser(), u);
      }
    });
    FileSystem.closeAllForUGI(ugi);
    return s;
  }
}
