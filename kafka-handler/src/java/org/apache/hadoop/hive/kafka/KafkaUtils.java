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

package org.apache.hadoop.hive.kafka;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.StringUtils;
import org.apache.hive.common.util.ReflectionUtil;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.OffsetMetadataTooLarge;
import org.apache.kafka.common.errors.SecurityDisabledException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.UnknownServerException;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Utils class for Kafka Storage handler plus some Constants.
 */
final class KafkaUtils {
  private final static Logger log = LoggerFactory.getLogger(KafkaUtils.class);
  private static final String JAAS_TEMPLATE = "com.sun.security.auth.module.Krb5LoginModule required "
      + "useKeyTab=true storeKey=true keyTab=\"%s\" principal=\"%s\";";
  private static final String JAAS_TEMPLATE_SCRAM =
      "org.apache.kafka.common.security.scram.ScramLoginModule required "
          + "username=\"%s\" password=\"%s\" serviceName=\"%s\" tokenauth=true;";
  static final Text KAFKA_DELEGATION_TOKEN_KEY = new Text("KAFKA_DELEGATION_TOKEN");
  private static final Set<String> SSL_CONFIG_KEYS =
      ImmutableSet.copyOf(new ConfigDef().withClientSslSupport().configKeys().keySet());

  private KafkaUtils() {
  }

  /**
   * Table property prefix used to inject kafka consumer properties, e.g "kafka.consumer.max.poll.records" = "5000"
   * this will lead to inject max.poll.records=5000 to the Kafka Consumer. NOT MANDATORY defaults to nothing
   */
  static final String CONSUMER_CONFIGURATION_PREFIX = "kafka.consumer";

  /**
   * Table property prefix used to inject kafka producer properties, e.g "kafka.producer.lingers.ms" = "100".
   */
  static final String PRODUCER_CONFIGURATION_PREFIX = "kafka.producer";

  /**
   * Set of Kafka properties that the user can not set via DDLs.
   */
  static final Set<String>
      FORBIDDEN_PROPERTIES =
      new HashSet<>(ImmutableList.of(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
          ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
          ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
          ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
          ProducerConfig.TRANSACTIONAL_ID_CONFIG,
          ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
          ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG));

  /**
   * @param configuration Job configs
   *
   * @return default consumer properties
   */
  static Properties consumerProperties(Configuration configuration) {
    final Properties props = new Properties();
    // we are managing the commit offset
    props.setProperty(CommonClientConfigs.CLIENT_ID_CONFIG, Utilities.getTaskId(configuration));
    props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    // we are seeking in the stream so no reset
    props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");
    String brokerEndPoint = configuration.get(KafkaTableProperties.HIVE_KAFKA_BOOTSTRAP_SERVERS.getName());
    if (brokerEndPoint == null || brokerEndPoint.isEmpty()) {
      throw new IllegalArgumentException("Kafka Broker End Point is missing Please set Config "
          + KafkaTableProperties.HIVE_KAFKA_BOOTSTRAP_SERVERS.getName());
    }
    props.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, brokerEndPoint);
    props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    //case Kerberos is On
    if (UserGroupInformation.isSecurityEnabled()) {
      addKerberosJaasConf(configuration, props);
    }

    // user can always override stuff, but SSL properties are derived from configuration, because they require local
    //   files. These need to modified afterwards. This works because these properties use the standard consumer prefix.
    props.putAll(extractExtraProperties(configuration, CONSUMER_CONFIGURATION_PREFIX));
    setupKafkaSslProperties(configuration, props);

    return props;
  }

  static void setupKafkaSslProperties(Configuration configuration, Properties props) {
    copySSLProperties(configuration, props);
    // Setup SSL via credentials keystore if necessary
    final String credKeystore = configuration.get(KafkaTableProperties.HIVE_KAFKA_SSL_CREDENTIAL_KEYSTORE.getName());
    if (!(credKeystore == null) && !credKeystore.isEmpty()) {
      final String truststorePasswdConfig =
          configuration.get(KafkaTableProperties.HIVE_KAFKA_SSL_TRUSTSTORE_PASSWORD.getName());
      final String keystorePasswdConfig =
          configuration.get(KafkaTableProperties.HIVE_KAFKA_SSL_KEYSTORE_PASSWORD.getName());
      final String keyPasswdConfig = configuration.get(KafkaTableProperties.HIVE_KAFKA_SSL_KEY_PASSWORD.getName());

      String resourcesDir = HiveConf.getVar(configuration, HiveConf.ConfVars.DOWNLOADED_RESOURCES_DIR);
      try {
        String truststoreLoc = configuration.get(KafkaTableProperties.HIVE_SSL_TRUSTSTORE_LOCATION_CONFIG.getName());
        Path truststorePath = new Path(truststoreLoc);
        props.setProperty(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG,
            new File(resourcesDir + "/" + truststorePath.getName()).getAbsolutePath());
        writeStoreToLocal(configuration, truststoreLoc, new File(resourcesDir).getAbsolutePath());

        final String truststorePasswd = Utilities.getPasswdFromKeystore(credKeystore, truststorePasswdConfig);
        props.setProperty(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, truststorePasswd);

        // ssl.keystore.password is only needed if two-way authentication is configured.
        if(!keystorePasswdConfig.isEmpty()) {
          log.info("Kafka keystore configured, configuring local keystore");
          String keystoreLoc = configuration.get(KafkaTableProperties.HIVE_SSL_KEYSTORE_LOCATION_CONFIG.getName());
          Path keystorePath = new Path(keystoreLoc);
          props.setProperty(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG,
              new File(resourcesDir + "/" + keystorePath.getName()).getAbsolutePath());
          writeStoreToLocal(configuration, keystoreLoc, new File(resourcesDir).getAbsolutePath());

          final String keystorePasswd = Utilities.getPasswdFromKeystore(credKeystore, keystorePasswdConfig);
          props.setProperty(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, keystorePasswd);
        }

        // ssl.key.password is optional for clients.
        if(!keyPasswdConfig.isEmpty()) {
          final String keyPasswd = Utilities.getPasswdFromKeystore(credKeystore, keyPasswdConfig);
          props.setProperty(SslConfigs.SSL_KEY_PASSWORD_CONFIG, keyPasswd);
        }
      } catch (IOException | URISyntaxException e) {
        throw new IllegalStateException("Unable to retrieve password from the credential keystore", e);
      }
    }
  }

  /**
   * Copies Kafka SSL properties from source configuration to target property map.
   * <p>
   * It only copies SSL properties that are present in the source and not present in the target. It is useful to
   * propagate global configurations to the Kafka client but also account for use-cases where table properties are not
   * using the Hive specific prefixes ({@link #CONSUMER_CONFIGURATION_PREFIX}, {@link #PRODUCER_CONFIGURATION_PREFIX}).
   * </p>
   * @param source the configuration from which we will get the properties 
   * @param target the property map to which we will set the properties
   */
  private static void copySSLProperties(Configuration source, Properties target) {
    for (String p : SSL_CONFIG_KEYS) {
      String v = source.get(p);
      if (v != null && !target.containsKey(p)) {
        target.setProperty(p, v);
      }
    }
  }

  private static void writeStoreToLocal(Configuration configuration, String hdfsLoc, String localDest)
      throws IOException, URISyntaxException {
    try {
      // Make sure the local resources directory is created
      File localDir = new File(localDest);
      if(!localDir.exists()) {
        if(!localDir.mkdirs()) {
          throw new IOException("Unable to create local directory, " + localDest);
        }
      }
      URI uri = new URI(hdfsLoc);
      FileSystem fs = FileSystem.get(new URI(hdfsLoc), configuration);
      fs.copyToLocalFile(new Path(uri.toString()), new Path(localDest));
    } catch (URISyntaxException e) {
      throw new IOException("Unable to download store", e);
    }
  }

  private static Map<String, String> extractExtraProperties(final Configuration configuration, String prefix) {
    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
    final Map<String, String> kafkaProperties = configuration.getValByRegex("^" + prefix + "\\..*");
    for (Map.Entry<String, String> entry : kafkaProperties.entrySet()) {
      String key = entry.getKey().substring(prefix.length() + 1);
      if (FORBIDDEN_PROPERTIES.contains(key)) {
        throw new IllegalArgumentException("Not suppose to set Kafka Property " + key);
      }
      builder.put(key, entry.getValue());
    }
    return builder.build();
  }

  static Properties producerProperties(Configuration configuration) {
    final String writeSemanticValue = configuration.get(KafkaTableProperties.WRITE_SEMANTIC_PROPERTY.getName());
    final KafkaOutputFormat.WriteSemantic writeSemantic = KafkaOutputFormat.WriteSemantic.valueOf(writeSemanticValue);
    final Properties properties = new Properties();
    String brokerEndPoint = configuration.get(KafkaTableProperties.HIVE_KAFKA_BOOTSTRAP_SERVERS.getName());
    if (brokerEndPoint == null || brokerEndPoint.isEmpty()) {
      throw new IllegalArgumentException("Kafka Broker End Point is missing Please set Config "
          + KafkaTableProperties.HIVE_KAFKA_BOOTSTRAP_SERVERS.getName());
    }
    properties.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, brokerEndPoint);
    //case Kerberos is On
    if (UserGroupInformation.isSecurityEnabled()) {
      addKerberosJaasConf(configuration, properties);
    }

    // user can always override stuff
    properties.putAll(extractExtraProperties(configuration, PRODUCER_CONFIGURATION_PREFIX));
    setupKafkaSslProperties(configuration, properties);

    String taskId = configuration.get("mapred.task.id", null);
    properties.setProperty(CommonClientConfigs.CLIENT_ID_CONFIG,
        taskId == null ? "random_" + UUID.randomUUID().toString() : taskId);
    switch (writeSemantic) {
    case AT_LEAST_ONCE:
      properties.setProperty(ProducerConfig.RETRIES_CONFIG, String.valueOf(Integer.MAX_VALUE));
      //The number of acknowledgments the producer requires the leader to have received before considering a request as
      //complete. Here all means from all replicas.
      properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
      break;
    case EXACTLY_ONCE:
      // Assuming that TaskId is ReducerId_attemptId. need the Reducer ID to fence out zombie kafka producers.
      String reducerId = getTaskId(configuration);
      //The number of acknowledgments the producer requires the leader to have received before considering a request as
      // complete, all means from all replicas.
      properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
      properties.setProperty(ProducerConfig.RETRIES_CONFIG, String.valueOf(Integer.MAX_VALUE));
      properties.setProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG, reducerId);
      //Producer set to be IDEMPOTENT eg ensure that send() retries are idempotent.
      properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
      break;
    default:
      throw new IllegalArgumentException("Unknown Semantic " + writeSemantic);
    }
    return properties;
  }

  @SuppressWarnings("SameParameterValue") static void copyDependencyJars(Configuration conf, Class<?>... classes)
      throws IOException {
    Set<String> jars = new HashSet<>();
    FileSystem localFs = FileSystem.getLocal(conf);
    jars.addAll(conf.getStringCollection("tmpjars"));
    jars.addAll(Arrays.stream(classes)
        .filter(Objects::nonNull)
        .map(clazz -> {
          String path = Utilities.jarFinderGetJar(clazz);
          if (path == null) {
            throw new RuntimeException("Could not find jar for class "
                + clazz
                + " in order to ship it to the cluster.");
          }
          try {
            if (!localFs.exists(new Path(path))) {
              throw new RuntimeException("Could not validate jar file " + path + " for class " + clazz);
            }
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
          return path;
        }).collect(Collectors.toList()));

    if (jars.isEmpty()) {
      return;
    }
    conf.set("tmpjars", StringUtils.arrayToString(jars.toArray(new String[0])));
  }

  static AbstractSerDe createDelegate(String className) {
    final Class<? extends AbstractSerDe> clazz;
    try {
      //noinspection unchecked
      clazz = (Class<? extends AbstractSerDe>) Class.forName(className);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
    // we are not setting conf thus null is okay
    return ReflectionUtil.newInstance(clazz, null);
  }

  static ProducerRecord<byte[], byte[]> toProducerRecord(String topic, KafkaWritable value) {
    return new ProducerRecord<>(topic,
        value.getPartition() != -1 ? value.getPartition() : null,
        value.getTimestamp() != -1L ? value.getTimestamp() : null,
        value.getRecordKey(),
        value.getValue());
  }

  /**
   * Check if the exception is Non-Retriable there a show stopper all we can do is clean and exit.
   * @param exception input exception object.
   * @return true if the exception is fatal thus we only can abort and rethrow the cause.
   */
  static boolean exceptionIsFatal(final Throwable exception) {
    final boolean
        securityException =
        exception instanceof AuthenticationException
            || exception instanceof AuthorizationException
            || exception instanceof SecurityDisabledException;

    final boolean
        communicationException =
        exception instanceof InvalidTopicException
            || exception instanceof UnknownServerException
            || exception instanceof SerializationException
            || exception instanceof OffsetMetadataTooLarge
            || exception instanceof IllegalStateException;

    return securityException || communicationException;
  }

  /**
   * Computes the kafka producer transaction id. The Tx id HAS to be the same across task restarts,
   * that is why we are excluding the attempt id by removing the last string after last `_`.
   * Assuming the taskId format is taskId_[m-r]_attemptId.
   *
   * @param hiveConf Hive Configuration.
   * @return the taskId without the attempt id.
   */
  static String getTaskId(Configuration hiveConf) {
    String id = Preconditions.checkNotNull(hiveConf.get("mapred.task.id", null));
    int index = id.lastIndexOf("_");
    if (index != -1) {
      return id.substring(0, index);
    }
    return id;
  }

  /**
   * Helper method that add Kerberos Jaas configs to the properties.
   * @param configuration Hive config containing kerberos key and principal
   * @param props properties to be populated
   */
  static void addKerberosJaasConf(Configuration configuration, Properties props) {
    //based on this https://kafka.apache.org/documentation/#security_jaas_client
    props.setProperty("security.protocol", "SASL_PLAINTEXT");
    props.setProperty("sasl.mechanism", "GSSAPI");
    props.setProperty("sasl.kerberos.service.name", "kafka");

    //Construct the principal/keytab
    String principalHost = HiveConf.getVar(configuration, HiveConf.ConfVars.HIVE_SERVER2_KERBEROS_PRINCIPAL);
    String keyTab = HiveConf.getVar(configuration, HiveConf.ConfVars.HIVE_SERVER2_KERBEROS_KEYTAB);
    // back to use LLAP keys if HS2 conf are not set or visible for the Task.
    if (principalHost == null || principalHost.isEmpty() || keyTab == null || keyTab.isEmpty()) {
      keyTab = HiveConf.getVar(configuration, HiveConf.ConfVars.LLAP_FS_KERBEROS_KEYTAB_FILE);
      principalHost = HiveConf.getVar(configuration, HiveConf.ConfVars.LLAP_FS_KERBEROS_PRINCIPAL);
    }

    String principal;
    try {
      principal = SecurityUtil.getServerPrincipal(principalHost, "0.0.0.0");
    } catch (IOException e) {
      log.error("Can not construct kerberos principal", e);
      throw new RuntimeException(e);
    }
    String jaasConf = String.format(JAAS_TEMPLATE, keyTab, principal);
    props.setProperty("sasl.jaas.config", jaasConf);

    if (configuration instanceof JobConf) {
      Credentials creds = ((JobConf) configuration).getCredentials();
      Token<?> token = creds.getToken(KAFKA_DELEGATION_TOKEN_KEY);

      if (token != null) {
        log.info("Kafka delegation token has been found: {}", token);
        props.setProperty("sasl.mechanism", "SCRAM-SHA-256");

        jaasConf = String.format(JAAS_TEMPLATE_SCRAM, new String(token.getIdentifier()),
            Base64.getEncoder().encodeToString(token.getPassword()), token.getService());
        props.setProperty("sasl.jaas.config", jaasConf);
      }
    }
    log.info("Kafka client running with following JAAS = [{}]", jaasConf);
  }

  /**
   * Returns the security protocol if one is defined in the properties and null otherwise.
   * <p>The following properties are examined to determine the protocol:</p>
   * <ol>
   *   <li>security.protocol</li>
   *   <li>kafka.consumer.security.protocol</li>
   *   <li>kafka.producer.security.protocol</li>
   * </ol>
   * <p>and the first non null/not empty is returned.</p>
   * <p>Defining multiple security protocols at the same time is invalid but this method is lenient and tries to pick
   * the most reasonable option.</p>
   * @param props the properties from which to obtain the protocol.
   * @return the security protocol if one is defined in the properties and null otherwise.
   */
  static SecurityProtocol securityProtocol(Properties props) {
    String[] securityProtocolConfigs = new String[] { CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
        CONSUMER_CONFIGURATION_PREFIX + "." + CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
        PRODUCER_CONFIGURATION_PREFIX + "." + CommonClientConfigs.SECURITY_PROTOCOL_CONFIG };
    for (String c : securityProtocolConfigs) {
      String v = props.getProperty(c);
      if (v != null && !v.isEmpty()) {
        return SecurityProtocol.forName(v);
      }
    }
    return null;
  }
}
