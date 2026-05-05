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

package org.apache.hive.kubernetes.operator.util;

import java.util.Map;

/**
 * Builds Hadoop-style XML configuration file content from a property map.
 * The output format matches standard Hadoop configuration files as used by
 * Hive, HDFS, and Tez.
 */
public final class HadoopXmlBuilder {

  private HadoopXmlBuilder() {
  }

  /**
   * Renders a property map as a Hadoop-style XML configuration string.
   *
   * @param properties key-value pairs to include in the configuration
   * @return XML string in Hadoop configuration format
   */
  public static String buildXml(Map<String, String> properties) {
    StringBuilder sb = new StringBuilder();
    sb.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
    sb.append("<?xml-stylesheet type=\"text/xsl\" href=\"configuration.xsl\"?>\n");
    sb.append("<configuration>\n");
    if (properties != null) {
      for (Map.Entry<String, String> entry : properties.entrySet()) {
        sb.append("  <property>\n");
        sb.append("    <name>").append(escapeXml(entry.getKey()))
            .append("</name>\n");
        sb.append("    <value>").append(escapeXml(entry.getValue()))
            .append("</value>\n");
        sb.append("  </property>\n");
      }
    }
    sb.append("</configuration>\n");
    return sb.toString();
  }

  private static String escapeXml(String value) {
    if (value == null) {
      return "";
    }
    return value
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace("\"", "&quot;")
        .replace("'", "&apos;");
  }
}
