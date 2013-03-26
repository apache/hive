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

package org.apache.hcatalog.mapreduce;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.thrift.DelegationTokenSelector;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.security.token.TokenSelector;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenIdentifier;
import org.apache.hcatalog.common.HCatConstants;
import org.apache.hcatalog.common.HCatUtil;
import org.apache.thrift.TException;

final class Security {
  
  // making sure this is not initialized unless needed
  private static final class LazyHolder {
    public static final Security INSTANCE = new Security();
  }

  private static Map<String, Token<? extends AbstractDelegationTokenIdentifier>> tokenMap = new HashMap<String, Token<? extends AbstractDelegationTokenIdentifier>>();
  
  public static Security getInstance() {
    return LazyHolder.INSTANCE;
  }

  // a signature string to associate with a HCatTableInfo - essentially
  // a concatenation of dbname, tablename and partition keyvalues.
  private String getTokenSignature(OutputJobInfo outputJobInfo) {
    StringBuilder result = new StringBuilder("");
    String dbName = outputJobInfo.getDatabaseName();
    if(dbName != null) {
      result.append(dbName);
    }
    String tableName = outputJobInfo.getTableName();
    if(tableName != null) {
      result.append("+" + tableName);
    }
    Map<String, String> partValues = outputJobInfo.getPartitionValues();
    if(partValues != null) {
      for(Entry<String, String> entry: partValues.entrySet()) {
        result.append("+" + entry.getKey() + "=" + entry.getValue());
      }
    }
    return result.toString();
  }

  void handleSecurity(
      Job job, 
      OutputJobInfo outputJobInfo,
      HiveMetaStoreClient client, 
      Configuration conf,
      boolean harRequested)
      throws IOException, MetaException, TException, Exception {
    if(UserGroupInformation.isSecurityEnabled()){
      UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
      // check if oozie has set up a hcat deleg. token - if so use it
      TokenSelector<? extends TokenIdentifier> tokenSelector = new DelegationTokenSelector();
      // TODO: will oozie use a "service" called "oozie" - then instead of
      // new Text() do new Text("oozie") below - if this change is made also
      // remember to do:
      //  job.getConfiguration().set(HCAT_KEY_TOKEN_SIGNATURE, "oozie");
      // Also change code in OutputCommitter.cleanupJob() to cancel the
      // token only if token.service is not "oozie" - remove the condition of
      // HCAT_KEY_TOKEN_SIGNATURE != null in that code.
      Token<? extends TokenIdentifier> token = tokenSelector.selectToken(
          new Text(), ugi.getTokens());
      if(token != null) {
  
        job.getCredentials().addToken(new Text(ugi.getUserName()),token);
  
      } else {
  
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
        if(tokenMap.get(tokenSignature) == null) {
          // get delegation tokens from hcat server and store them into the "job"
          // These will be used in to publish partitions to
          // hcat normally in OutputCommitter.commitJob()
          // when the JobTracker in Hadoop MapReduce starts supporting renewal of 
          // arbitrary tokens, the renewer should be the principal of the JobTracker
          tokenMap.put(tokenSignature, HCatUtil.extractThriftToken(
              client.getDelegationToken(ugi.getUserName()),
              tokenSignature));
        }
  
        String jcTokenSignature = "jc."+tokenSignature;
        if (harRequested){
          if(tokenMap.get(jcTokenSignature) == null) {
            tokenMap.put(jcTokenSignature,
                HCatUtil.getJobTrackerDelegationToken(conf,ugi.getUserName()));
          }
        }
        
        job.getCredentials().addToken(new Text(ugi.getUserName() + tokenSignature),
            tokenMap.get(tokenSignature));
        // this will be used by the outputcommitter to pass on to the metastore client
        // which in turn will pass on to the TokenSelector so that it can select
        // the right token.
        job.getConfiguration().set(HCatConstants.HCAT_KEY_TOKEN_SIGNATURE, tokenSignature);
  
        if (harRequested){
          job.getCredentials().addToken(new Text(ugi.getUserName() + jcTokenSignature),
              tokenMap.get(jcTokenSignature));
  
          job.getConfiguration().set(
              HCatConstants.HCAT_KEY_JOBCLIENT_TOKEN_SIGNATURE, jcTokenSignature);
          job.getConfiguration().set(
              HCatConstants.HCAT_KEY_JOBCLIENT_TOKEN_STRFORM, 
              tokenMap.get(jcTokenSignature).encodeToUrlString());
          //          LOG.info("Set hive dt["+tokenSignature+"]");
          //          LOG.info("Set jt dt["+jcTokenSignature+"]");
        }
      }
  }
  }
}
