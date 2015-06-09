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
import java.lang.reflect.Method;
import java.util.Collection;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.admin.SecurityOperations;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper class to hold common methods across the InputFormat, OutputFormat and StorageHandler.
 */
public class HiveAccumuloHelper {
  private static final Logger log = LoggerFactory.getLogger(HiveAccumuloHelper.class);
  // Constant from Accumulo's DelegationTokenImpl
  public static final Text ACCUMULO_SERVICE = new Text("ACCUMULO_AUTH_TOKEN");

  // Constants for DelegationToken reflection to continue to support 1.6
  private static final String DELEGATION_TOKEN_CONFIG_CLASS_NAME =
      "org.apache.accumulo.core.client.admin.DelegationTokenConfig";
  private static final String DELEGATION_TOKEN_IMPL_CLASS_NAME =
      "org.apache.accumulo.core.client.impl.DelegationTokenImpl";
  private static final String GET_DELEGATION_TOKEN_METHOD_NAME = "getDelegationToken";
  private static final String GET_IDENTIFIER_METHOD_NAME = "getIdentifier";
  private static final String GET_PASSWORD_METHOD_NAME = "getPassword";
  private static final String GET_SERVICE_NAME_METHOD_NAME = "getServiceName";

  // Constants for ClientConfiguration and setZooKeeperInstance reflection
  // to continue to support 1.5
  private static final String CLIENT_CONFIGURATION_CLASS_NAME =
      "org.apache.accumulo.core.client.ClientConfiguration";
  private static final String LOAD_DEFAULT_METHOD_NAME = "loadDefault";
  private static final String SET_PROPERTY_METHOD_NAME = "setProperty";
  private static final String INSTANCE_ZOOKEEPER_HOST = "instance.zookeeper.host";
  private static final String INSTANCE_NAME = "instance.name";
  private static final String INSTANCE_RPC_SASL_ENABLED = "instance.rpc.sasl.enabled";
  private static final String SET_ZOOKEEPER_INSTANCE_METHOD_NAME = "setZooKeeperInstance";

  // Constants for unwrapping the DelegationTokenStub into a DelegationTokenImpl
  private static final String CONFIGURATOR_BASE_CLASS_NAME =
      "org.apache.accumulo.core.client.mapreduce.lib.impl.ConfiguratorBase";
  private static final String UNWRAP_AUTHENTICATION_TOKEN_METHOD_NAME = "unwrapAuthenticationToken";

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
    Token<?> accumuloToken = null;
    Collection<Token<? extends TokenIdentifier>> tokens = user.getTokens();
    for (Token<?> token : tokens) {
      if (ACCUMULO_SERVICE.equals(token.getKind())) {
        accumuloToken = token;
        break;
      }
    }

    // If we didn't find the Token, we can't proceed. Log the tokens for debugging.
    if (null == accumuloToken) {
      log.error("Could not find accumulo token in user: " + tokens);
      throw new IOException("Could not find Accumulo Token in user's tokens");
    }

    // Add the Hadoop token back to the Job, the configuration still has the necessary
    // Accumulo token information.
    mergeTokenIntoJobConf(jobConf, accumuloToken);
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
   * Obtain a DelegationToken from Accumulo in a backwards compatible manner.
   *
   * @param conn
   *          The Accumulo connector
   * @return The DelegationToken instance
   * @throws IOException
   *           If the token cannot be obtained
   */
  public AuthenticationToken getDelegationToken(Connector conn) throws IOException {
    try {
      Class<?> clz = JavaUtils.loadClass(DELEGATION_TOKEN_CONFIG_CLASS_NAME);
      // DelegationTokenConfig delegationTokenConfig = new DelegationTokenConfig();
      Object delegationTokenConfig = clz.newInstance();

      SecurityOperations secOps = conn.securityOperations();

      Method getDelegationTokenMethod = secOps.getClass().getMethod(
          GET_DELEGATION_TOKEN_METHOD_NAME, clz);

      // secOps.getDelegationToken(delegationTokenConfig)
      return (AuthenticationToken) getDelegationTokenMethod.invoke(secOps, delegationTokenConfig);
    } catch (Exception e) {
      throw new IOException("Failed to obtain DelegationToken from Accumulo", e);
    }
  }

  public Token<? extends TokenIdentifier> getHadoopToken(AuthenticationToken delegationToken)
      throws IOException {
    try {
      // DelegationTokenImpl class
      Class<?> delegationTokenClass = JavaUtils.loadClass(DELEGATION_TOKEN_IMPL_CLASS_NAME);
      // Methods on DelegationToken
      Method getIdentifierMethod = delegationTokenClass.getMethod(GET_IDENTIFIER_METHOD_NAME);
      Method getPasswordMethod = delegationTokenClass.getMethod(GET_PASSWORD_METHOD_NAME);
      Method getServiceNameMethod = delegationTokenClass.getMethod(GET_SERVICE_NAME_METHOD_NAME);

      // Treat the TokenIdentifier implementation as the abstract class to avoid dependency issues
      // AuthenticationTokenIdentifier identifier = delegationToken.getIdentifier();
      TokenIdentifier identifier = (TokenIdentifier) getIdentifierMethod.invoke(delegationToken);

      // new Token<AuthenticationTokenIdentifier>(identifier.getBytes(),
      //     delegationToken.getPassword(), identifier.getKind(), delegationToken.getServiceName());
      return new Token<TokenIdentifier>(identifier.getBytes(), (byte[])
          getPasswordMethod.invoke(delegationToken), identifier.getKind(),
          (Text) getServiceNameMethod.invoke(delegationToken));
    } catch (Exception e) {
      throw new IOException("Failed to create Hadoop token from Accumulo DelegationToken", e);
    }
  }

  /**
   * Construct a <code>ClientConfiguration</code> instance in a backwards-compatible way. Allows us
   * to support Accumulo 1.5
   *
   * @param zookeepers
   *          ZooKeeper hosts
   * @param instanceName
   *          Instance name
   * @param useSasl
   *          Is SASL enabled
   * @return A ClientConfiguration instance
   * @throws IOException
   *           If the instance fails to be created
   */
  public Object getClientConfiguration(String zookeepers, String instanceName, boolean useSasl)
      throws IOException {
    try {
      // Construct a new instance of ClientConfiguration
      Class<?> clientConfigClass = JavaUtils.loadClass(CLIENT_CONFIGURATION_CLASS_NAME);
      Method loadDefaultMethod = clientConfigClass.getMethod(LOAD_DEFAULT_METHOD_NAME);
      Object clientConfig = loadDefaultMethod.invoke(null);

      // Set instance and zookeeper hosts
      Method setPropertyMethod = clientConfigClass.getMethod(SET_PROPERTY_METHOD_NAME,
          String.class, Object.class);
      setPropertyMethod.invoke(clientConfig, INSTANCE_ZOOKEEPER_HOST, zookeepers);
      setPropertyMethod.invoke(clientConfig, INSTANCE_NAME, instanceName);

      if (useSasl) {
        // Defaults to not using SASL, set true if SASL is being used
        setPropertyMethod.invoke(clientConfig, INSTANCE_RPC_SASL_ENABLED, true);
      }

      return clientConfig;
    } catch (Exception e) {
      String msg = "Failed to instantiate and invoke methods on ClientConfiguration";
      log.error(msg, e);
      throw new IOException(msg, e);
    }
  }

  /**
   * Wrapper around <code>setZooKeeperInstance(Configuration, ClientConfiguration)</code> which only
   * exists in 1.6.0 and newer. Support backwards compat.
   *
   * @param jobConf
   *          The JobConf
   * @param inputOrOutputFormatClass
   *          The InputFormat or OutputFormat class
   * @param zookeepers
   *          ZooKeeper hosts
   * @param instanceName
   *          Accumulo instance name
   * @param useSasl
   *          Is SASL enabled
   * @throws IOException
   *           When invocation of the method fails
   */
  public void setZooKeeperInstance(JobConf jobConf, Class<?> inputOrOutputFormatClass, String
      zookeepers, String instanceName, boolean useSasl) throws IOException {
    try {
      Class<?> clientConfigClass = JavaUtils.loadClass(CLIENT_CONFIGURATION_CLASS_NAME);

      // get the ClientConfiguration
      Object clientConfig = getClientConfiguration(zookeepers, instanceName, useSasl);

      // AccumuloOutputFormat.setZooKeeperInstance(JobConf, ClientConfiguration) or
      // AccumuloInputFormat.setZooKeeperInstance(JobConf, ClientConfiguration)
      Method setZooKeeperMethod = inputOrOutputFormatClass.getMethod(
          SET_ZOOKEEPER_INSTANCE_METHOD_NAME, JobConf.class, clientConfigClass);
      setZooKeeperMethod.invoke(null, jobConf, clientConfig);
    } catch (Exception e) {
      throw new IOException("Failed to invoke setZooKeeperInstance method", e);
    }
  }

  /**
   * Wrapper around <code>ConfiguratorBase.unwrapAuthenticationToken</code> which only exists in
   * 1.7.0 and new. Uses reflection to not break compat.
   *
   * @param jobConf
   *          JobConf object
   * @param token
   *          The DelegationTokenStub instance
   * @return A DelegationTokenImpl created from the Token in the Job's credentials
   * @throws IOException
   *           If the token fails to be unwrapped
   */
  public AuthenticationToken unwrapAuthenticationToken(JobConf jobConf, AuthenticationToken token)
      throws IOException {
    try {
      Class<?> configuratorBaseClass = JavaUtils.loadClass(CONFIGURATOR_BASE_CLASS_NAME);
      Method unwrapAuthenticationTokenMethod = configuratorBaseClass.getMethod(
          UNWRAP_AUTHENTICATION_TOKEN_METHOD_NAME, JobConf.class, AuthenticationToken.class);
      // ConfiguratorBase.unwrapAuthenticationToken(conf, token);
      return (AuthenticationToken) unwrapAuthenticationTokenMethod.invoke(null, jobConf, token);
    } catch (Exception e) {
      throw new IOException("Failed to unwrap AuthenticationToken", e);
    }
  }
}
