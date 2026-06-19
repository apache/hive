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
package org.apache.hive.beeline.hs2connection;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

/**
 * This class implements HS2ConnectionFileParser for the named url configuration file named
 * beeline-site.xml. The class looks for this file in ${user.home}/.beeline, ${HIVE_CONF_DIR} or
 * /etc/conf/hive in that order and uses the first file found in the above locations.
 */
public class BeelineSiteParser implements HS2ConnectionFileParser {
  /**
   * Prefix string used for named jdbc uri configs
   */
  public static final String BEELINE_CONNECTION_NAMED_JDBC_URL_PREFIX = "beeline.hs2.jdbc.url.";
  /**
   * Property key used to provide the default named jdbc uri in the config file
   */
  public static final String DEFAULT_NAMED_JDBC_URL_PROPERTY_KEY = "default";

  public static final String DEFAULT_BEELINE_SITE_FILE_NAME = "beeline-site.xml";
  public static final String DEFAULT_BEELINE_SITE_LOCATION =
      System.getProperty("user.home") + File.separator
          + (System.getProperty("os.name").toLowerCase().indexOf("windows") != -1 ? "" : ".")
          + "beeline" + File.separator;
  public static final String ETC_HIVE_CONF_LOCATION =
      File.separator + "etc" + File.separator + "hive" + File.separator + "conf";

  private final List<String> locations = new ArrayList<>();
  private static final Logger log = LoggerFactory.getLogger(BeelineSiteParser.class);

  public BeelineSiteParser() {
    // file locations to be searched in the correct order
    locations.add(DEFAULT_BEELINE_SITE_LOCATION + DEFAULT_BEELINE_SITE_FILE_NAME);
    if (System.getenv("HIVE_CONF_DIR") != null) {
      locations
          .add(System.getenv("HIVE_CONF_DIR") + File.separator + DEFAULT_BEELINE_SITE_FILE_NAME);
    }
    locations.add(ETC_HIVE_CONF_LOCATION + File.separator + DEFAULT_BEELINE_SITE_FILE_NAME);
  }

  @VisibleForTesting
  BeelineSiteParser(List<String> testLocations) {
    if(testLocations == null) {
      return;
    }
    locations.addAll(testLocations);
  }

  @Override
  public Properties getConnectionProperties() throws BeelineSiteParseException {
    Properties props = new Properties();
    String fileLocation = getFileLocation();
    if (fileLocation == null) {
      log.debug("Could not find Beeline configuration file: {}", DEFAULT_BEELINE_SITE_FILE_NAME);
      return props;
    }
    log.info("Beeline configuration file at: {}", fileLocation);
    // load the properties from config file
    Configuration conf = new Configuration(false);
    conf.addResource(new Path(new File(fileLocation).toURI()));
    try {
      for (Entry<String, String> kv : conf) {
        String key = kv.getKey();
        if (key.startsWith(BEELINE_CONNECTION_NAMED_JDBC_URL_PREFIX)) {
          // using conf.get(key) to help with variable substitution
          props.setProperty(key.substring(BEELINE_CONNECTION_NAMED_JDBC_URL_PREFIX.length()),
              conf.get(key));
        }
      }
    } catch (Exception e) {
      throw new BeelineSiteParseException(e.getMessage(), e);
    }
    return props;
  }

  public Properties getConnectionProperties(String propertyValue) throws BeelineSiteParseException {
    Properties props = new Properties();
    String fileLocation = getFileLocation();
    if (fileLocation == null) {
      log.debug("Could not find Beeline configuration file: {}", DEFAULT_BEELINE_SITE_FILE_NAME);
      return props;
    }
    log.info("Beeline configuration file at: {}", fileLocation);
    // load the properties from config file
    Configuration conf = new Configuration(false);
    conf.addResource(new Path(new File(fileLocation).toURI()));
    try {
      for (Entry<String, String> kv : conf) {
        String key = kv.getKey();
        if (key.startsWith(BEELINE_CONNECTION_NAMED_JDBC_URL_PREFIX)
            && (propertyValue.equalsIgnoreCase(kv.getValue()))) {
          props.setProperty(key.substring(BEELINE_CONNECTION_NAMED_JDBC_URL_PREFIX.length()),
              conf.get(key));
        }
      }
    } catch (Exception e) {
      throw new BeelineSiteParseException(e.getMessage(), e);
    }
    return props;
  }

  @Override
  public boolean configExists() {
    return (getFileLocation() != null);
  }
  /*
   * This method looks in locations specified above and returns the first location where the file
   * exists. If the file does not exist in any one of the locations it returns null
   */
  String getFileLocation() {
    for (String location : locations) {
      if (new File(location).exists()) {
        return location;
      }
    }
    return null;
  }
}
