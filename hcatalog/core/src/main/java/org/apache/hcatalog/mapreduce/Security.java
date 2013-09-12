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

package org.apache.hcatalog.mapreduce;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.thrift.DelegationTokenSelector;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.security.token.TokenSelector;
import org.apache.hcatalog.common.HCatConstants;
import org.apache.hcatalog.common.HCatUtil;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @deprecated Use/modify {@link org.apache.hive.hcatalog.mapreduce.Security} instead
 */
final class Security {

  private static final Logger LOG = LoggerFactory.getLogger(HCatOutputFormat.class);

  // making sure this is not initialized unless needed
  private static final class LazyHolder {
    public static final Security INSTANCE = new Security();
  }

  public static Security getInstance() {
    return LazyHolder.INSTANCE;
  }

  boolean isSecurityEnabled() {
    try {
      Method m = UserGroupInformation.class.getMethod("isSecurityEnabled");
      return (Boolean) m.invoke(null, (Object[]) null);
    } catch (NoSuchMethodException e) {
      LOG.info("Security is not supported by this version of hadoop.", e);
    } catch (InvocationTargetException e) {
      String msg = "Failed to call isSecurityEnabled()";
      LOG.info(msg, e);
      throw new IllegalStateException(msg, e);
    } catch (IllegalAccessException e) {
      String msg = "Failed to call isSecurityEnabled()";
      LOG.info(msg, e);
      throw new IllegalStateException(msg, e);
    }
    return false;
  }

  // a signature string to associate with a HCatTableInfo - essentially
  // a concatenation of dbname, tablename and partition keyvalues.
  String getTokenSignature(OutputJobInfo outputJobInfo) {
    StringBuilder result = new StringBuilder("");
    String dbName = outputJobInfo.getDatabaseName();
    if (dbName != null) {
      result.append(dbName);
    }
    String tableName = outputJobInfo.getTableName();
    if (tableName != null) {
      result.append("." + tableName);
    }
    Map<String, String> partValues = outputJobInfo.getPartitionValues();
    if (partValues != null) {
      for (Entry<String, String> entry : partValues.entrySet()) {
        result.append("/");
        result.append(entry.getKey());
        result.append("=");
        result.append(entry.getValue());
      }

    }
    return result.toString();
  }

  void handleSecurity(
    Credentials credentials,
    OutputJobInfo outputJobInfo,
    HiveMetaStoreClient client,
    Configuration conf,
    boolean harRequested)
    throws IOException, MetaException, TException, Exception {
    if (UserGroupInformation.isSecurityEnabled()) {
      UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
      // check if oozie has set up a hcat deleg. token - if so use it
      TokenSelector<? extends TokenIdentifier> hiveTokenSelector = new DelegationTokenSelector();
      //Oozie does not change the service field of the token
      //hence by default token generation will have a value of "new Text("")"
      //HiveClient will look for a use TokenSelector.selectToken() with service
      //set to empty "Text" if hive.metastore.token.signature property is set to null
      Token<? extends TokenIdentifier> hiveToken = hiveTokenSelector.selectToken(
        new Text(), ugi.getTokens());
      if (hiveToken == null) {
        // we did not get token set up by oozie, let's get them ourselves here.
        // we essentially get a token per unique Output HCatTableInfo - this is
        // done because through Pig, setOutput() method is called multiple times
        // We want to only get the token once per unique output HCatTableInfo -
        // we cannot just get one token since in multi-query case (> 1 store in 1 job)
        // or the case when a single pig script results in > 1 jobs, the single
        // token will get cancelled by the output committer and the subsequent
        // stores will fail - by tying the token with the concatenation of
        // dbname, tablename and partition keyvalues of the output
        // TableInfo, we can have as many tokens as there are stores and the TokenSelector
        // will correctly pick the right tokens which the committer will use and
        // cancel.
        String tokenSignature = getTokenSignature(outputJobInfo);
        // get delegation tokens from hcat server and store them into the "job"
        // These will be used in to publish partitions to
        // hcat normally in OutputCommitter.commitJob()
        // when the JobTracker in Hadoop MapReduce starts supporting renewal of
        // arbitrary tokens, the renewer should be the principal of the JobTracker
        hiveToken = HCatUtil.extractThriftToken(client.getDelegationToken(ugi.getUserName()), tokenSignature);

        if (harRequested) {
          TokenSelector<? extends TokenIdentifier> jtTokenSelector =
            new org.apache.hadoop.mapreduce.security.token.delegation.DelegationTokenSelector();
          Token jtToken = jtTokenSelector.selectToken(org.apache.hadoop.security.SecurityUtil.buildTokenService(
            ShimLoader.getHadoopShims().getHCatShim().getResourceManagerAddress(conf)), ugi.getTokens());
          if (jtToken == null) {
            //we don't need to cancel this token as the TokenRenewer for JT tokens
            //takes care of cancelling them
            credentials.addToken(
              new Text("hcat jt token"),
              HCatUtil.getJobTrackerDelegationToken(conf, ugi.getUserName())
            );
          }
        }

        credentials.addToken(new Text(ugi.getUserName() + "_" + tokenSignature), hiveToken);
        // this will be used by the outputcommitter to pass on to the metastore client
        // which in turn will pass on to the TokenSelector so that it can select
        // the right token.
        conf.set(HCatConstants.HCAT_KEY_TOKEN_SIGNATURE, tokenSignature);
      }
    }
  }

  void handleSecurity(
    Job job,
    OutputJobInfo outputJobInfo,
    HiveMetaStoreClient client,
    Configuration conf,
    boolean harRequested)
    throws IOException, MetaException, TException, Exception {
    handleSecurity(job.getCredentials(), outputJobInfo, client, conf, harRequested);
  }

  // we should cancel hcat token if it was acquired by hcat
  // and not if it was supplied (ie Oozie). In the latter
  // case the HCAT_KEY_TOKEN_SIGNATURE property in the conf will not be set
  void cancelToken(HiveMetaStoreClient client, JobContext context) throws IOException, MetaException {
    String tokenStrForm = client.getTokenStrForm();
    if (tokenStrForm != null && context.getConfiguration().get(HCatConstants.HCAT_KEY_TOKEN_SIGNATURE) != null) {
      try {
        client.cancelDelegationToken(tokenStrForm);
      } catch (TException e) {
        String msg = "Failed to cancel delegation token";
        LOG.error(msg, e);
        throw new IOException(msg, e);
      }
    }
  }

}
