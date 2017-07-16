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
package org.apache.hadoop.hive.accumulo;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.admin.DelegationTokenConfig;
import org.apache.accumulo.core.client.impl.DelegationTokenImpl;
import org.apache.accumulo.core.client.mapred.AccumuloOutputFormat;
import org.apache.accumulo.core.client.mapreduce.lib.impl.InputConfigurator;
import org.apache.accumulo.core.client.mapreduce.lib.impl.OutputConfigurator;
import org.apache.accumulo.core.client.mapred.AccumuloInputFormat;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.fate.Fate;
import org.apache.accumulo.start.Main;
import org.apache.accumulo.trace.instrument.Tracer;
import org.apache.accumulo.core.client.impl.AuthenticationTokenIdentifier;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper class to hold common methods across the InputFormat, OutputFormat and StorageHandler.
 */
public class HiveAccumuloHelper {
  private static final Logger log = LoggerFactory.getLogger(HiveAccumuloHelper.class);
  // Constant from Accumulo's AuthenticationTokenIdentifier
  public static final Text ACCUMULO_SERVICE = new Text("ACCUMULO_AUTH_TOKEN");

  /**
   * Extract the appropriate Token for Accumulo from the provided {@code user} and add it to the
   * {@link JobConf}'s credentials.
   *
   * @param user
   *          User containing tokens
   * @param jobConf
   *          The configuration for the job
   * @throws IOException
   *           If the correct token is not found or the Token fails to be merged with the
   *           configuration
   */
  public void addTokenFromUserToJobConf(UserGroupInformation user, JobConf jobConf)
      throws IOException {
    checkNotNull(user, "Provided UGI was null");
    checkNotNull(jobConf, "JobConf was null");

    // Accumulo token already in Configuration, but the Token isn't in the Job credentials like the
    // AccumuloInputFormat expects
    Token<?> accumuloToken = getAccumuloToken(user);

    // If we didn't find the Token, we can't proceed. Log the tokens for debugging.
    if (null == accumuloToken) {
      log.error("Could not find accumulo token in user: " + user.getTokens());
      throw new IOException("Could not find Accumulo Token in user's tokens");
    }

    // Add the Hadoop token back to the Job, the configuration still has the necessary
    // Accumulo token information.
    mergeTokenIntoJobConf(jobConf, accumuloToken);
  }

  public Token<?> getAccumuloToken(UserGroupInformation user) {
    checkNotNull(user, "Provided UGI was null");
    Collection<Token<? extends TokenIdentifier>> tokens = user.getTokens();
    for (Token<?> token : tokens) {
      if (ACCUMULO_SERVICE.equals(token.getKind())) {
        return token;
      }
    }
    return null;
  }

  /**
   * Merge the provided <code>Token</code> into the JobConf.
   *
   * @param jobConf
   *          JobConf to merge token into
   * @param accumuloToken
   *          The Token
   * @throws IOException
   *           If the merging fails
   */
  public void mergeTokenIntoJobConf(JobConf jobConf, Token<?> accumuloToken) throws IOException {
    JobConf accumuloJobConf = new JobConf(jobConf);
    accumuloJobConf.getCredentials().addToken(accumuloToken.getService(), accumuloToken);

    // Merge them together.
    ShimLoader.getHadoopShims().mergeCredentials(jobConf, accumuloJobConf);
  }

  /**
   * Obtain a DelegationToken from Accumulo.
   *
   * @param conn
   *          The Accumulo connector
   * @return The DelegationToken instance
   * @throws IOException
   *           If the token cannot be obtained
   */
  public AuthenticationToken getDelegationToken(Connector conn) throws IOException {
    try {
      DelegationTokenConfig config = new DelegationTokenConfig();
      return conn.securityOperations().getDelegationToken(config);
    } catch (AccumuloException | AccumuloSecurityException e) {
      throw new IOException("Failed to obtain DelegationToken", e);
    }
  }

  public Token<? extends TokenIdentifier> getHadoopToken(AuthenticationToken token)
      throws IOException {
    if (!(token instanceof DelegationTokenImpl)) {
      throw new IOException("Expected a DelegationTokenImpl but found " +
          (token != null ? token.getClass() : "null"));
    }
    DelegationTokenImpl dt = (DelegationTokenImpl) token;
    try {
      AuthenticationTokenIdentifier identifier = dt.getIdentifier();

      return new Token<AuthenticationTokenIdentifier>(identifier.getBytes(),
          dt.getPassword(), identifier.getKind(), dt.getServiceName());
    } catch (Exception e) {
      throw new IOException("Failed to create Hadoop token from Accumulo DelegationToken", e);
    }
  }

  /**
   * Construct a <code>ClientConfiguration</code> instance.
   *
   * @param zookeepers
   *          ZooKeeper hosts
   * @param instanceName
   *          Instance name
   * @param useSasl
   *          Is SASL enabled
   * @return A ClientConfiguration instance
   */
  public ClientConfiguration getClientConfiguration(String zookeepers, String instanceName, boolean useSasl) {
    return ClientConfiguration.loadDefault().withInstance(instanceName).withZkHosts(zookeepers).withSasl(useSasl);
  }

  public void updateInputFormatConfWithAccumuloToken(JobConf jobConf, UserGroupInformation currentUser,
      AccumuloConnectionParameters cnxnParams) throws IOException {
    updateConfWithAccumuloToken(jobConf, currentUser, cnxnParams, true);
  }

  public void updateOutputFormatConfWithAccumuloToken(JobConf jobConf, UserGroupInformation currentUser,
      AccumuloConnectionParameters cnxnParams) throws IOException {
    updateConfWithAccumuloToken(jobConf, currentUser, cnxnParams, false);
  }

  void updateConfWithAccumuloToken(JobConf jobConf, UserGroupInformation currentUser,
      AccumuloConnectionParameters cnxnParams, boolean isInputFormat) throws IOException {
    if (getAccumuloToken(currentUser) != null) {
      addTokenFromUserToJobConf(currentUser, jobConf);
    } else {
      try {
        Connector connector = cnxnParams.getConnector();
        // If we have Kerberos credentials, we should obtain the delegation token
        AuthenticationToken token = getDelegationToken(connector);

        // Send the DelegationToken down to the Configuration for Accumulo to use
        if (isInputFormat) {
          setInputFormatConnectorInfo(jobConf, cnxnParams.getAccumuloUserName(), token);
        } else {
          setOutputFormatConnectorInfo(jobConf, cnxnParams.getAccumuloUserName(), token);
        }

        // Convert the Accumulo token in a Hadoop token
        Token<? extends TokenIdentifier> accumuloToken = getHadoopToken(token);

        // Add the Hadoop token to the JobConf
        mergeTokenIntoJobConf(jobConf, accumuloToken);

        // Make sure the UGI contains the token too for good measure
        if (!currentUser.addToken(accumuloToken)) {
          throw new IOException("Failed to add Accumulo Token to UGI");
        }

        try {
          addTokenFromUserToJobConf(currentUser, jobConf);
        } catch (IOException e) {
          throw new IOException("Current user did not contain necessary delegation Tokens " + currentUser, e);
        }
      } catch (AccumuloException | AccumuloSecurityException e) {
        throw new IOException("Failed to acquire Accumulo DelegationToken", e);
      }
    }
  }

  public boolean hasKerberosCredentials(UserGroupInformation ugi) {
    // Allows mocking in testing.
    return ugi.getAuthenticationMethod() == AuthenticationMethod.KERBEROS;
  }

  /**
   * Calls {@link AccumuloInputFormat#setConnectorInfo(JobConf, String, AuthenticationToken)},
   * suppressing exceptions due to setting the configuration multiple times.
   */
  public void setInputFormatConnectorInfo(JobConf conf, String username, AuthenticationToken token)
      throws AccumuloSecurityException {
    try {
      AccumuloInputFormat.setConnectorInfo(conf, username, token);
    } catch (IllegalStateException e) {
      // AccumuloInputFormat complains if you re-set an already set value. We just don't care.
      log.debug("Ignoring exception setting Accumulo Connector instance for user " + username, e);
    }
  }

  /**
   * Calls {@link AccumuloOutputFormat#setConnectorInfo(JobConf, String, AuthenticationToken)}
   * suppressing exceptions due to setting the configuration multiple times.
   */
  public void setOutputFormatConnectorInfo(JobConf conf, String username, AuthenticationToken token)
      throws AccumuloSecurityException {
    try {
      AccumuloOutputFormat.setConnectorInfo(conf, username, token);
    } catch (IllegalStateException e) {
      // AccumuloOutputFormat complains if you re-set an already set value. We just don't care.
      log.debug("Ignoring exception setting Accumulo Connector instance for user " + username, e);
    }
  }

  /**
   * Calls {@link AccumuloInputFormat#setZooKeeperInstance(JobConf, ClientConfiguration)},
   * suppressing exceptions due to setting the configuration multiple times.
   */
  public void setInputFormatZooKeeperInstance(JobConf conf, String instanceName, String zookeepers,
      boolean isSasl) throws IOException {
    try {
      ClientConfiguration clientConf = getClientConfiguration(zookeepers, instanceName, isSasl);
      AccumuloInputFormat.setZooKeeperInstance(conf, clientConf);
    } catch (IllegalStateException ise) {
      // AccumuloInputFormat complains if you re-set an already set value. We just don't care.
      log.debug("Ignoring exception setting ZooKeeper instance of " + instanceName + " at "
          + zookeepers, ise);
    }
  }

  /**
   * Calls {@link AccumuloOutputFormat#setZooKeeperInstance(JobConf, ClientConfiguration)},
   * suppressing exceptions due to setting the configuration multiple times.
   */
  public void setOutputFormatZooKeeperInstance(JobConf conf, String instanceName, String zookeepers,
      boolean isSasl) throws IOException {
    try {
      ClientConfiguration clientConf = getClientConfiguration(zookeepers, instanceName, isSasl);
      AccumuloOutputFormat.setZooKeeperInstance(conf, clientConf);
    } catch (IllegalStateException ise) {
      // AccumuloOutputFormat complains if you re-set an already set value. We just don't care.
      log.debug("Ignoring exception setting ZooKeeper instance of " + instanceName + " at "
          + zookeepers, ise);
    }
  }

  /**
   * Calls {@link AccumuloInputFormat#setMockInstance(JobConf, String)}, suppressing exceptions due
   * to setting the configuration multiple times.
   */
  public void setInputFormatMockInstance(JobConf conf, String instanceName) {
    try {
      AccumuloInputFormat.setMockInstance(conf, instanceName);
    } catch (IllegalStateException e) {
      // AccumuloInputFormat complains if you re-set an already set value. We just don't care.
      log.debug("Ignoring exception setting mock instance of " + instanceName, e);
    }
  }

  /**
   * Calls {@link AccumuloOutputFormat#setMockInstance(JobConf, String)}, suppressing exceptions
   * due to setting the configuration multiple times.
   */
  public void setOutputFormatMockInstance(JobConf conf, String instanceName) {
    try {
      AccumuloOutputFormat.setMockInstance(conf, instanceName);
    } catch (IllegalStateException e) {
      // AccumuloOutputFormat complains if you re-set an already set value. We just don't care.
      log.debug("Ignoring exception setting mock instance of " + instanceName, e);
    }
  }

  /**
   * Sets all jars requried by Accumulo input/output tasks in the configuration to be dynamically
   * loaded when the task is executed.
   */
  public void loadDependentJars(Configuration conf) {
    @SuppressWarnings("deprecation")
    List<Class<?>> classesToLoad = new ArrayList<>(Arrays.asList(Tracer.class, Fate.class, Connector.class, Main.class, ZooKeeper.class, AccumuloStorageHandler.class));
    try {
      classesToLoad.add(Class.forName("org.apache.htrace.Trace"));
    } catch (Exception e) {
      log.warn("Failed to load class for HTrace jar, trying to continue", e);
    }
    try {
      Utils.addDependencyJars(conf, classesToLoad);
    } catch (IOException e) {
      log.error("Could not add necessary Accumulo dependencies to classpath", e);
    }
  }

  /**
   * Obtains an Accumulo DelegationToken and sets it in the configuration for input and output jobs.
   * The Accumulo token is converted into a Hadoop-style token and returned to the caller.
   *
   * @return A Hadoop-style token which contains the Accumulo DelegationToken
   */
  public Token<? extends TokenIdentifier> setConnectorInfoForInputAndOutput(AccumuloConnectionParameters params, Connector conn, Configuration conf) throws Exception {
    // Obtain a delegation token from Accumulo
    AuthenticationToken token = getDelegationToken(conn);

    // Make sure the Accumulo token is set in the Configuration (only a stub of the Accumulo
    // AuthentiationToken is serialized, not the entire token). configureJobConf may be
    // called multiple times with the same JobConf which results in an error from Accumulo
    // MapReduce API. Catch the error, log a debug message and just keep going
    try {
      InputConfigurator.setConnectorInfo(AccumuloInputFormat.class, conf,
          params.getAccumuloUserName(), token);
    } catch (IllegalStateException e) {
      // The implementation balks when this method is invoked multiple times
      log.debug("Ignoring IllegalArgumentException about re-setting connector information");
    }
    try {
      OutputConfigurator.setConnectorInfo(AccumuloOutputFormat.class, conf,
          params.getAccumuloUserName(), token);
    } catch (IllegalStateException e) {
      // The implementation balks when this method is invoked multiple times
      log.debug("Ignoring IllegalArgumentException about re-setting connector information");
    }

    return getHadoopToken(token);
  }
}
