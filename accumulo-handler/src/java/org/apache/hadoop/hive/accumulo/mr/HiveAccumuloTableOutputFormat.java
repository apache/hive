/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.accumulo.mr;

import java.io.IOException;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.mapred.AccumuloOutputFormat;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Mutation;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.accumulo.AccumuloConnectionParameters;
import org.apache.hadoop.hive.accumulo.HiveAccumuloHelper;
import org.apache.hadoop.hive.accumulo.serde.AccumuloSerDeParameters;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.util.Progressable;

import com.google.common.base.Preconditions;

/**
 *
 */
public class HiveAccumuloTableOutputFormat extends AccumuloOutputFormat {

  protected final HiveAccumuloHelper helper = new HiveAccumuloHelper();

  @Override
  public void checkOutputSpecs(FileSystem ignored, JobConf job) throws IOException {
    configureAccumuloOutputFormat(job);

    super.checkOutputSpecs(ignored, job);
  }

  @Override
  public RecordWriter<Text,Mutation> getRecordWriter(FileSystem ignored, JobConf job, String name, Progressable progress) throws IOException {
    configureAccumuloOutputFormat(job);

    return super.getRecordWriter(ignored, job, name, progress);
  }

  protected void configureAccumuloOutputFormat(JobConf job) throws IOException {
    AccumuloConnectionParameters cnxnParams = new AccumuloConnectionParameters(job);

    final String tableName = job.get(AccumuloSerDeParameters.TABLE_NAME);

    // Make sure we actually go the table name
    Preconditions.checkNotNull(tableName,
        "Expected Accumulo table name to be provided in job configuration");

    // Set the necessary Accumulo information
    try {
      if (cnxnParams.useMockInstance()) {
        setAccumuloMockInstance(job, cnxnParams.getAccumuloInstanceName());
      } else {
        // Accumulo instance name with ZK quorum
        setAccumuloZooKeeperInstance(job, cnxnParams.getAccumuloInstanceName(),
            cnxnParams.getZooKeepers(), cnxnParams.useSasl());
      }

      // Extract the delegation Token from the UGI and add it to the job
      // The AccumuloOutputFormat will look for it there.
      if (cnxnParams.useSasl()) {
        UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
        if (!ugi.hasKerberosCredentials()) {
          helper.addTokenFromUserToJobConf(ugi, job);
        } else {
          // Still in the local JVM, can use Kerberos credentials
          try {
            Connector connector = cnxnParams.getConnector();
            AuthenticationToken token = helper.getDelegationToken(connector);

            // Send the DelegationToken down to the Configuration for Accumulo to use
            setConnectorInfo(job, cnxnParams.getAccumuloUserName(), token);

            // Convert the Accumulo token in a Hadoop token
            Token<? extends TokenIdentifier> accumuloToken = helper.getHadoopToken(token);

            log.info("Adding Hadoop Token for Accumulo to Job's Credentials");

            // Add the Hadoop token to the JobConf
            helper.mergeTokenIntoJobConf(job, accumuloToken);

            // Make sure the UGI contains the token too for good measure
            if (!ugi.addToken(accumuloToken)) {
              throw new IOException("Failed to add Accumulo Token to UGI");
            }
          } catch (AccumuloException | AccumuloSecurityException e) {
            throw new IOException("Failed to acquire Accumulo DelegationToken", e);
          }
        }
      } else {
        setAccumuloConnectorInfo(job, cnxnParams.getAccumuloUserName(),
            new PasswordToken(cnxnParams.getAccumuloPassword()));
      }

      // Set the table where we're writing this data
      setDefaultAccumuloTableName(job, tableName);
    } catch (AccumuloSecurityException e) {
      log.error("Could not connect to Accumulo with provided credentials", e);
      throw new IOException(e);
    }
  }

  // Non-static methods to wrap the static AccumuloOutputFormat methods to enable testing

  protected void setAccumuloConnectorInfo(JobConf conf, String username, AuthenticationToken token)
      throws AccumuloSecurityException {
    try {
      AccumuloOutputFormat.setConnectorInfo(conf, username, token);
    } catch (IllegalStateException e) {
      // AccumuloOutputFormat complains if you re-set an already set value. We just don't care.
      log.debug("Ignoring exception setting Accumulo Connector instance for user " + username, e);
    }
  }

  @SuppressWarnings("deprecation")
  protected void setAccumuloZooKeeperInstance(JobConf conf, String instanceName, String zookeepers,
      boolean isSasl) throws IOException {
    try {
      if (isSasl) {
        // Reflection to support Accumulo 1.5. Remove when Accumulo 1.5 support is dropped
        // 1.6 works with the deprecated 1.5 method, but must use reflection for 1.7-only
        // SASL support
        helper.setZooKeeperInstance(conf, AccumuloOutputFormat.class, zookeepers, instanceName,
            isSasl);
      } else {
        AccumuloOutputFormat.setZooKeeperInstance(conf, instanceName, zookeepers);
      }
    } catch (IllegalStateException ise) {
      // AccumuloOutputFormat complains if you re-set an already set value. We just don't care.
      log.debug("Ignoring exception setting ZooKeeper instance of " + instanceName + " at "
          + zookeepers, ise);
    }
  }

  protected void setAccumuloMockInstance(JobConf conf, String instanceName) {
    try {
      AccumuloOutputFormat.setMockInstance(conf, instanceName);
    } catch (IllegalStateException e) {
      // AccumuloOutputFormat complains if you re-set an already set value. We just don't care.
      log.debug("Ignoring exception setting mock instance of " + instanceName, e);
    }
  }

  protected void setDefaultAccumuloTableName(JobConf conf, String tableName) {
    AccumuloOutputFormat.setDefaultTableName(conf, tableName);
  }
}
