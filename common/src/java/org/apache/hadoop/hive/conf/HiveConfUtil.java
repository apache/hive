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

package org.apache.hadoop.hive.conf;

import com.google.common.collect.Iterables;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.classification.InterfaceAudience.Private;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.hive.common.util.HiveStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.stream.Stream;

import static org.apache.hive.common.util.HiveStringUtils.COMMA;
import static org.apache.hive.common.util.HiveStringUtils.EQUALS;

/**
 * Hive Configuration utils
 */
@Private
public class HiveConfUtil {
  private static final String CLASS_NAME = HiveConfUtil.class.getName();
  private static final Logger LOG = LoggerFactory.getLogger(CLASS_NAME);
  /**
   * Check if metastore is being used in embedded mode.
   * This utility function exists so that the logic for determining the mode is same
   * in HiveConf and HiveMetaStoreClient
   * @param msUri - metastore server uri
   * @return
   */
  public static boolean isEmbeddedMetaStore(String msUri) {
    return (msUri == null) ? true : msUri.trim().isEmpty();
  }

  /**
   * Dumps all HiveConf for debugging.  Convenient to dump state at process start up and log it
   * so that in later analysis the values of all variables is known
   */
  public static StringBuilder dumpConfig(HiveConf conf) {
    StringBuilder sb = new StringBuilder("START========\"HiveConf()\"========\n");
    sb.append("hiveDefaultUrl=").append(conf.getHiveDefaultLocation()).append('\n');
    sb.append("hiveSiteURL=").append(HiveConf.getHiveSiteLocation()).append('\n');
    sb.append("hiveServer2SiteUrl=").append(HiveConf.getHiveServer2SiteLocation()).append('\n');
    sb.append("hivemetastoreSiteUrl=").append(HiveConf.getMetastoreSiteLocation()).append('\n');
    dumpConfig(conf, sb);
    return sb.append("END========\"new HiveConf()\"========\n");
  }

  /**
   * Getting the set of the hidden configurations
   * @param configuration The original configuration
   * @return The list of the configuration values to hide
   */
  public static Set<String> getHiddenSet(Configuration configuration) {
    Set<String> hiddenSet = new HashSet<String>();
    String hiddenListStr = HiveConf.getVar(configuration, HiveConf.ConfVars.HIVE_CONF_HIDDEN_LIST);
    if (hiddenListStr != null) {
      for (String entry : hiddenListStr.split(",")) {
        hiddenSet.add(entry.trim());
      }
    }
    return hiddenSet;
  }

  /**
   * Getting the set of locked configurations
   * @param configuration The original configuration
   * @return The list of the configuration values to be locked
   */
  public static Set<String> getLockedSet(Configuration configuration) {
    Set<String> lockedSet = new HashSet<>();
    String lockedListStr = HiveConf.getVar(configuration, ConfVars.HIVE_CONF_LOCKED_LIST);
    if (lockedListStr != null) {
      for (String entry : lockedListStr.split(",")) {
        lockedSet.add(entry.trim());
      }
    }
    return lockedSet;
  }

  /**
   * Strips hidden config entries from configuration
   * @param conf The configuration to strip from
   * @param hiddenSet The values to strip
   */
  public static void stripConfigurations(Configuration conf, Set<String> hiddenSet) {

    // Find all configurations where the key contains any string from hiddenSet
    Iterable<Map.Entry<String, String>> matching =
        Iterables.filter(conf, confEntry -> {
          for (String name : hiddenSet) {
            if (confEntry.getKey().startsWith(name)) {
              return true;
            }
          }
          return false;
        });

    // Remove the value of every key found matching
    matching.forEach(entry -> conf.set(entry.getKey(), StringUtils.EMPTY));
  }

  /**
   * Searches the given configuration object and replaces all the configuration values for keys
   * defined hive.conf.hidden.list by empty String
   *
   * @param conf - Configuration object which needs to be modified to remove sensitive keys
   */
  public static void stripConfigurations(Configuration conf) {
    Set<String> hiddenSet = getHiddenSet(conf);
    stripConfigurations(conf, hiddenSet);
  }

  public static void dumpConfig(Configuration originalConf, StringBuilder sb) {
    Set<String> hiddenSet = getHiddenSet(originalConf);
    sb.append("Values omitted for security reason if present: ").append(hiddenSet).append("\n");
    Configuration conf = new Configuration(originalConf);
    stripConfigurations(conf, hiddenSet);

    Iterator<Map.Entry<String, String>> configIter = conf.iterator();
    List<Map.Entry<String, String>> configVals = new ArrayList<>();
    while(configIter.hasNext()) {
      configVals.add(configIter.next());
    }
    Collections.sort(configVals, new Comparator<Map.Entry<String, String>>() {
      @Override
      public int compare(Map.Entry<String, String> ent, Map.Entry<String, String> ent2) {
        return ent.getKey().compareTo(ent2.getKey());
      }
    });
    for(Map.Entry<String, String> entry : configVals) {
      //use get() to make sure variable substitution works
      if(entry.getKey().toLowerCase().contains("path")) {
        StringTokenizer st = new StringTokenizer(conf.get(entry.getKey()), File.pathSeparator);
        sb.append(entry.getKey()).append("=\n");
        while(st.hasMoreTokens()) {
          sb.append("    ").append(st.nextToken()).append(File.pathSeparator).append('\n');
        }
      }
      else {
        sb.append(entry.getKey()).append('=').append(conf.get(entry.getKey())).append('\n');
      }
    }
  }

  /**
   * Updates the job configuration with the job specific credential provider information available
   * in the HiveConf.It uses the environment variables HADOOP_CREDSTORE_PASSWORD or
   * HIVE_JOB_CREDSTORE_PASSWORD to get the custom password for all the keystores configured in the
   * provider path. This usage of environment variables is similar in lines with Hadoop credential
   * provider mechanism for getting the keystore passwords. The other way of communicating the
   * password is through a file which stores the password in clear-text which needs to be readable
   * by all the consumers and therefore is not supported.
   *
   *<ul>
   * <li>If HIVE_SERVER2_JOB_CREDENTIAL_PROVIDER_PATH is set in the hive configuration this method
   * overrides the MR job configuration property hadoop.security.credential.provider.path with its
   * value. If not set then it does not change the value of hadoop.security.credential.provider.path
   * <li>In order to choose the password for the credential provider we check :
   *
   *   (1) if job credential provider path HIVE_SERVER2_JOB_CREDENTIAL_PROVIDER_PATH is set we check if
   *       HIVE_SERVER2_JOB_CREDSTORE_PASSWORD_ENVVAR is set. If it is set we use it.
   *   (2) If password is not set using (1) above we use HADOOP_CREDSTORE_PASSWORD if it is set.
   *   (3) If none of those are set, we do not set any password in the MR task environment. In this
   *       case the hadoop credential provider should use the default password of "none" automatically
   *</ul>
   * @param jobConf - job specific configuration
   */
  public static void updateJobCredentialProviders(Configuration jobConf) {
    if(jobConf == null) {
      return;
    }

    String jobKeyStoreLocation = jobConf.get(HiveConf.ConfVars.HIVE_SERVER2_JOB_CREDENTIAL_PROVIDER_PATH.varname);
    String oldKeyStoreLocation = jobConf.get(Constants.HADOOP_CREDENTIAL_PROVIDER_PATH_CONFIG);

    if (StringUtils.isNotBlank(jobKeyStoreLocation)) {
      jobConf.set(Constants.HADOOP_CREDENTIAL_PROVIDER_PATH_CONFIG, jobKeyStoreLocation);
      LOG.debug("Setting job conf credstore location to " + jobKeyStoreLocation
          + " previous location was " + oldKeyStoreLocation);
    }

    updateCredentialProviderPasswordForJobs(jobConf);
  }

  public static void updateCredentialProviderPasswordForJobs(Configuration jobConf) {
    String credstorePassword = getJobCredentialProviderPassword(jobConf);
    if (credstorePassword != null) {
      String execEngine = jobConf.get(ConfVars.HIVE_EXECUTION_ENGINE.varname);

      if ("mr".equalsIgnoreCase(execEngine) || "tez".equalsIgnoreCase(execEngine)) {
        // if the execution engine is MR/Tez set the map/reduce env with the credential store password
        Collection<String> redactedProperties =
            jobConf.getStringCollection(MRJobConfig.MR_JOB_REDACTED_PROPERTIES);
        /*
         * There are AM + task related environment props below, used for both MR and Tez.
         * Hiveserver2 copies some of them while creating the vertex in
         * DagUtils.createVertex -> setTaskEnvironment(getContainerEnvironment(conf)).
         * So for clarity's sake, TEZ_TASK_LAUNCH_ENV is not added here to avoid confusion of
         * taking care of task env twice. Comments below clarifies which execution engine relies on which property.
         * "MR -> Tez" means that DagUtils copies them to tez tasks' environment.
         */
        Stream.of(
            JobConf.MAPRED_MAP_TASK_ENV, // MR -> Tez
            JobConf.MAPRED_REDUCE_TASK_ENV, // MR -> Tez
            MRJobConfig.MR_AM_ADMIN_USER_ENV, // MR
            TezConfiguration.TEZ_AM_LAUNCH_ENV) // Tez

            .forEach(property -> {
              addKeyValuePair(jobConf, property,
                  Constants.HADOOP_CREDENTIAL_PASSWORD_ENVVAR, credstorePassword);
              redactedProperties.add(property);
            });

        // Hide sensitive configuration values from MR HistoryUI by telling MR to redact the following list.
        jobConf.set(MRJobConfig.MR_JOB_REDACTED_PROPERTIES,
            StringUtils.join(redactedProperties, COMMA));
      }
    }
  }

  /*
   * If HIVE_SERVER2_JOB_CREDSTORE_LOCATION is set check HIVE_SERVER2_JOB_CREDSTORE_PASSWORD_ENVVAR before
   * checking HADOOP_CREDENTIAL_PASSWORD_ENVVAR
   */
  public static String getJobCredentialProviderPassword(Configuration conf) {
    String jobKeyStoreLocation =
        conf.get(HiveConf.ConfVars.HIVE_SERVER2_JOB_CREDENTIAL_PROVIDER_PATH.varname);
    String password = null;
    if(StringUtils.isNotBlank(jobKeyStoreLocation)) {
      password = System.getenv(Constants.HIVE_SERVER2_JOB_CREDSTORE_PASSWORD_ENVVAR);
      if (StringUtils.isNotBlank(password)) {
        return password;
      }
    }
    password = System.getenv(Constants.HADOOP_CREDENTIAL_PASSWORD_ENVVAR);
    if (StringUtils.isNotBlank(password)) {
      return password;
    }
    return null;
  }

  /**
   * Sets a "keyName=newKeyValue" pair to a jobConf to a given property.
   * If the property is empty, it simply inserts keyName=newKeyValue,
   * if it's already filled, it takes care of appending or replacing it in the currently present value.
   * The property in jobConf contains a value like: "key1=value1,key2=value2".
   * @param jobConf
   * @param property
   * @param keyName
   * @param newKeyValue
   */
  private static void addKeyValuePair(Configuration jobConf, String property, String keyName, String newKeyValue) {
    String existingValue = jobConf.get(property);

    if (StringUtils.isBlank(existingValue)) {
      jobConf.set(property, (keyName + EQUALS + newKeyValue));
      return;
    }
    String propertyValue = HiveStringUtils.insertValue(keyName, newKeyValue, existingValue);
    jobConf.set(property, propertyValue);
  }

  @SuppressWarnings("unchecked")
  public static void copyFromProperties(Properties propSource, HiveConf confTarget) {
    Enumeration<String> props = (Enumeration<String>) propSource.propertyNames();
    while (props.hasMoreElements()) {
      String key = props.nextElement();
      confTarget.set(key, propSource.getProperty(key));
    }
  }
}
