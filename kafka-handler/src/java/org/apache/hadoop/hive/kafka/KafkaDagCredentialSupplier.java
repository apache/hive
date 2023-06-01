/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.kafka;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.tez.DagCredentialSupplier;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.token.Token;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateDelegationTokenOptions;
import org.apache.kafka.clients.admin.CreateDelegationTokenResult;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.security.token.delegation.DelegationToken;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public class KafkaDagCredentialSupplier implements DagCredentialSupplier {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaDagCredentialSupplier.class);
  private static final Text KAFKA_DELEGATION_TOKEN_KEY = new Text("KAFKA_DELEGATION_TOKEN");

  @Override
  public Token<?> obtainToken(BaseWork work, Set<TableDesc> fileSinkTableDescs, Configuration conf) {
    if(!(work instanceof MapWork)){
      return null;
    }
    Map<String, PartitionDesc> partitions = ((MapWork) work).getAliasToPartnInfo();

    // We don't need to iterate on all partitions, and check the same TableDesc.
    PartitionDesc partition = partitions.values().stream().findFirst().orElse(null);
    if (partition != null) {
      TableDesc tableDesc = partition.getTableDesc();
      if (collectKafkaDelegationTokenForTableDesc(tableDesc)) {
        // don't collect delegation token again, if it was already successful
        return getKafkaDelegationTokenForBrokers(conf, tableDesc);
      }
    }

    for (TableDesc tableDesc : fileSinkTableDescs) {
      if (collectKafkaDelegationTokenForTableDesc(tableDesc)) {
        // don't collect delegation token again, if it was already successful
        return getKafkaDelegationTokenForBrokers(conf, tableDesc);
      }
    }
    return null;
  }

  @Override
  public Text getTokenAlias() {
    return KAFKA_DELEGATION_TOKEN_KEY;
  }

  /**
   * Returns whether we should collect delegation tokens for kafka in the scope of a TableDesc.
   * If "security.protocol" is set to "PLAINTEXT", we don't need to collect delegation token at all.
   * @param tableDesc
   * @return true if we should collect a token for the specified table and false otherwise.
   */
  private boolean collectKafkaDelegationTokenForTableDesc(TableDesc tableDesc) {
    String kafkaBrokers = (String) tableDesc.getProperties().get("kafka.bootstrap.servers"); //FIXME: KafkaTableProperties
    String consumerSecurityProtocol = (String) tableDesc.getProperties().get(
        "kafka.consumer." + CommonClientConfigs.SECURITY_PROTOCOL_CONFIG);
    String producerSecurityProtocol = (String) tableDesc.getProperties().get(
        "kafka.producer." + CommonClientConfigs.SECURITY_PROTOCOL_CONFIG);
    return kafkaBrokers != null && !kafkaBrokers.isEmpty()
        && !CommonClientConfigs.DEFAULT_SECURITY_PROTOCOL.equalsIgnoreCase(consumerSecurityProtocol)
        && !CommonClientConfigs.DEFAULT_SECURITY_PROTOCOL.equalsIgnoreCase(producerSecurityProtocol);
  }

  private Token<?> getKafkaDelegationTokenForBrokers(Configuration conf, TableDesc tableDesc) {
    String kafkaBrokers = (String) tableDesc.getProperties().get("kafka.bootstrap.servers");
    LOG.info("Getting kafka credentials for brokers: {}", kafkaBrokers);

    String keytab = HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_SERVER2_KERBEROS_KEYTAB);
    String principal = HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_SERVER2_KERBEROS_PRINCIPAL);
    try {
      principal = SecurityUtil.getServerPrincipal(principal, "0.0.0.0");
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    Properties config = new Properties();
    config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokers);
    config.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");

    String jaasConfig =
        String.format("%s %s %s %s serviceName=\"%s\" keyTab=\"%s\" principal=\"%s\";",
            "com.sun.security.auth.module.Krb5LoginModule required", "debug=true", "useKeyTab=true",
            "storeKey=true", "kafka", keytab, principal);
    config.put(SaslConfigs.SASL_JAAS_CONFIG, jaasConfig);

    LOG.debug("Jaas config for requesting kafka credentials: {}", jaasConfig);
    AdminClient admin = AdminClient.create(config);

    CreateDelegationTokenOptions createDelegationTokenOptions = new CreateDelegationTokenOptions();
    CreateDelegationTokenResult createResult =
        admin.createDelegationToken(createDelegationTokenOptions);
    DelegationToken token;
    try {
      token = createResult.delegationToken().get();
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException("Exception while getting kafka delegation tokens", e);
    }
    LOG.info("Got kafka delegation token: {}", token);
    return new Token<>(token.tokenInfo().tokenId().getBytes(), token.hmac(), null, new Text("kafka"));
  }

}
