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
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class HS2ConnectionFileUtils {

  public static String getUrl(Properties props) throws BeelineHS2ConnectionFileParseException {
    if (props == null || props.isEmpty()) {
      return null;
    }
    // use remove instead of get so that it is not parsed again
    // in the for loop below
    String urlPrefix = (String) props.remove(HS2ConnectionFileParser.URL_PREFIX_PROPERTY_KEY);
    if (urlPrefix == null || urlPrefix.isEmpty()) {
      throw new BeelineHS2ConnectionFileParseException("url_prefix parameter cannot be empty");
    }

    String hosts = (String) props.remove(HS2ConnectionFileParser.HOST_PROPERTY_KEY);
    if (hosts == null || hosts.isEmpty()) {
      throw new BeelineHS2ConnectionFileParseException("hosts parameter cannot be empty");
    }
    String defaultDB = (String) props.remove(HS2ConnectionFileParser.DEFAULT_DB_PROPERTY_KEY);
    if (defaultDB == null) {
      defaultDB = "default";
    }
    // collect the hiveConfList and HiveVarList separately so that they can be
    // appended once all the session list are added to the url
    String hiveConfProperties = "";
    if (props.containsKey(HS2ConnectionFileParser.HIVE_CONF_PROPERTY_KEY)) {
      hiveConfProperties = extractHiveVariables(
          (String) props.remove(HS2ConnectionFileParser.HIVE_CONF_PROPERTY_KEY), true);
    }

    String hiveVarProperties = "";
    if (props.containsKey(HS2ConnectionFileParser.HIVE_VAR_PROPERTY_KEY)) {
      hiveVarProperties = extractHiveVariables(
          (String) props.remove(HS2ConnectionFileParser.HIVE_VAR_PROPERTY_KEY), false);
    }
    StringBuilder urlSb = new StringBuilder();
    urlSb.append(urlPrefix.trim());
    urlSb.append(hosts.trim());
    urlSb.append(File.separator);
    urlSb.append(defaultDB.trim());
    List<String> keys = new ArrayList<>(props.stringPropertyNames());
    // sorting the keys from the properties helps to create
    // a deterministic url which is tested for various configuration in
    // TestHS2ConnectionConfigFileManager
    Collections.sort(keys);
    for (String propertyName : keys) {
      urlSb.append(";");
      urlSb.append(propertyName);
      urlSb.append("=");
      urlSb.append(props.getProperty(propertyName));
    }
    if (!hiveConfProperties.isEmpty()) {
      urlSb.append(hiveConfProperties.toString());
    }
    if (!hiveVarProperties.isEmpty()) {
      urlSb.append(hiveVarProperties.toString());
    }
    return urlSb.toString();
  }

  private static String extractHiveVariables(String propertyValue, boolean isHiveConf)
      throws BeelineHS2ConnectionFileParseException {
    StringBuilder hivePropertiesList = new StringBuilder();
    String delimiter;
    if (isHiveConf) {
      delimiter = "?";
    } else {
      delimiter = "#";
    }
    hivePropertiesList.append(delimiter);
    addPropertyValues(propertyValue, hivePropertiesList);
    return hivePropertiesList.toString();
  }

  private static void addPropertyValues(String value, StringBuilder hivePropertiesList)
      throws BeelineHS2ConnectionFileParseException {
    // There could be multiple keyValuePairs separated by comma
    String[] values = value.split(",");
    boolean first = true;
    for (String keyValuePair : values) {
      String[] keyValue = keyValuePair.split("=");
      if (keyValue.length != 2) {
        throw new BeelineHS2ConnectionFileParseException("Unable to parse " + keyValuePair
            + " in hs2 connection config file");
      }
      if (!first) {
        hivePropertiesList.append(";");
      }
      first = false;
      hivePropertiesList.append(keyValue[0].trim());
      hivePropertiesList.append("=");
      hivePropertiesList.append(keyValue[1].trim());
    }
  }
}
