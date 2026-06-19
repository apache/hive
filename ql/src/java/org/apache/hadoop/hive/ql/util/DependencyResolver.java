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

package org.apache.hadoop.hive.ql.util;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.io.File;
import java.io.IOException;

import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;
import org.slf4j.LoggerFactory;

import groovy.grape.Grape;
import groovy.lang.GroovyClassLoader;


public class DependencyResolver {

  private static final String HIVE_HOME = "HIVE_HOME";
  private static final String HIVE_CONF_DIR = "HIVE_CONF_DIR";
  private String ivysettingsPath;
  private static LogHelper _console = new LogHelper(LoggerFactory.getLogger("DependencyResolver"));

  public DependencyResolver() {

    // Check if HIVE_CONF_DIR is defined
    if (System.getenv().containsKey(HIVE_CONF_DIR)) {
      ivysettingsPath = System.getenv().get(HIVE_CONF_DIR) + "/ivysettings.xml";
    }

    // If HIVE_CONF_DIR is not defined or file is not found in HIVE_CONF_DIR then check HIVE_HOME/conf
    if (ivysettingsPath == null || !(new File(ivysettingsPath).exists())) {
      if (System.getenv().containsKey(HIVE_HOME)) {
        ivysettingsPath = System.getenv().get(HIVE_HOME) + "/conf/ivysettings.xml";
      }
    }

    // If HIVE_HOME is not defined or file is not found in HIVE_HOME/conf then load default ivysettings.xml from class loader
    if (ivysettingsPath == null || !(new File(ivysettingsPath).exists())) {
      URL ivysetttingsResource = ClassLoader.getSystemResource("ivysettings.xml");
      if (ivysetttingsResource != null){
        ivysettingsPath = ivysetttingsResource.getFile();
        _console.printInfo("ivysettings.xml file not found in HIVE_HOME or HIVE_CONF_DIR," + ivysettingsPath + " will be used");
      }
    }

  }

  /**
   *
   * @param uri
   * @return List of URIs of downloaded jars
   * @throws URISyntaxException
   * @throws IOException
   */
  public List<URI> downloadDependencies(URI uri) throws URISyntaxException, IOException {
    Map<String, Object> dependencyMap = new HashMap<String, Object>();
    String authority = uri.getAuthority();
    if (authority == null) {
      throw new URISyntaxException(authority, "Invalid url: Expected 'org:module:version', found null");
    }
    String[] authorityTokens = authority.split(":");

    if (authorityTokens.length != 3) {
      throw new URISyntaxException(authority, "Invalid url: Expected 'org:module:version', found " + authority);
    }

    dependencyMap.put("org", authorityTokens[0]);
    dependencyMap.put("module", authorityTokens[1]);
    dependencyMap.put("version", authorityTokens[2]);
    Map<String, Object> queryMap = parseQueryString(uri.getQuery());
    if (queryMap != null) {
      dependencyMap.putAll(queryMap);
    }
    return grab(dependencyMap);
  }

  /**
   * @param queryString
   * @return queryMap Map which contains grape parameters such as transitive, exclude, ext and classifier.
   * Example: Input:  ext=jar&exclude=org.mortbay.jetty:jetty&transitive=true
   *          Output:  {[ext]:[jar], [exclude]:{[group]:[org.mortbay.jetty], [module]:[jetty]}, [transitive]:[true]}
   * @throws URISyntaxException
   */
  private Map<String, Object> parseQueryString(String queryString) throws URISyntaxException {
    if (queryString == null || queryString.isEmpty()) {
      return null;
    }
    List<Map<String, String>> excludeList = new LinkedList<Map<String, String>>();
    Map<String, Object> queryMap = new HashMap<String, Object>();
    String[] mapTokens = queryString.split("&");
    for (String tokens : mapTokens) {
      String[] mapPair = tokens.split("=");
      if (mapPair.length != 2) {
        throw new RuntimeException("Invalid query string: " + queryString);
      }
      if (mapPair[0].equals("exclude")) {
        excludeList.addAll(computeExcludeList(mapPair[1]));
      } else if (mapPair[0].equals("transitive")) {
        if (mapPair[1].toLowerCase().equals("true")) {
          queryMap.put(mapPair[0], true);
        } else {
          queryMap.put(mapPair[0], false);
        }
      } else {
        queryMap.put(mapPair[0], mapPair[1]);
      }
    }
    if (!excludeList.isEmpty()) {
      queryMap.put("exclude", excludeList);
    }
    return queryMap;
  }

  private List<Map<String, String>> computeExcludeList(String excludeString) throws URISyntaxException {
    String excludes[] = excludeString.split(",");
    List<Map<String, String>> excludeList = new LinkedList<Map<String, String>>();
    for (String exclude : excludes) {
      Map<String, String> tempMap = new HashMap<String, String>();
      String args[] = exclude.split(":");
      if (args.length != 2) {
        throw new URISyntaxException(excludeString,
            "Invalid exclude string: expected 'org:module,org:module,..', found " + excludeString);
      }
      tempMap.put("group", args[0]);
      tempMap.put("module", args[1]);
      excludeList.add(tempMap);
    }
    return excludeList;
  }

  /**
   *
   * @param dependencies
   * @return List of URIs of downloaded jars
   * @throws IOException
   */
  private List<URI> grab(Map<String, Object> dependencies) throws IOException {
    Map<String, Object> args = new HashMap<String, Object>();
    URI[] localUrls;

    //grape expects excludes key in args map
    if (dependencies.containsKey("exclude")) {
      args.put("excludes", dependencies.get("exclude"));
    }

    //Set transitive to true by default
    if (!dependencies.containsKey("transitive")) {
      dependencies.put("transitive", true);
    }

    args.put("classLoader", new GroovyClassLoader());
    System.setProperty("grape.config", ivysettingsPath);
    localUrls = Grape.resolve(args, dependencies);
    if (localUrls == null) {
      throw new IOException("Not able to download all the dependencies..");
    }
    return Arrays.asList(localUrls);
  }
}
