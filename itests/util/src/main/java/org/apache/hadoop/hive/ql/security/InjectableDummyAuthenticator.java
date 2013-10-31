/**
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
package org.apache.hadoop.hive.ql.security;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaStore.HMSHandler;
import org.apache.hadoop.hive.ql.metadata.HiveException;

/**
 *
 * InjectableDummyAuthenticator - An implementation of HiveMetastoreAuthenticationProvider
 * that wraps another Authenticator, but when asked to inject a user provided username
 * and groupnames, does so. This can be toggled back and forth to use in testing
 */
public class InjectableDummyAuthenticator implements HiveMetastoreAuthenticationProvider {

  private static String userName;
  private static List<String> groupNames;
  private static boolean injectMode;
  private static Class<? extends HiveMetastoreAuthenticationProvider> hmapClass =
      HadoopDefaultMetastoreAuthenticator.class;
  private HiveMetastoreAuthenticationProvider hmap;

  public static void injectHmapClass(Class<? extends HiveMetastoreAuthenticationProvider> clazz){
    hmapClass = clazz;
  }

  public static void injectUserName(String user){
    userName = user;
  }

  public static void injectGroupNames(List<String> groups){
    groupNames = groups;
  }

  public static void injectMode(boolean mode){
    injectMode = mode;
  }

  @Override
  public String getUserName() {
    if (injectMode){
      return userName;
    } else {
      return hmap.getUserName();
    }
  }

  @Override
  public List<String> getGroupNames() {
    if (injectMode) {
      return groupNames;
    } else {
      return hmap.getGroupNames();
    }
  }

  @Override
  public Configuration getConf() {
    return hmap.getConf();
  }

  @Override
  public void setConf(Configuration config) {
    try {
      hmap = (HiveMetastoreAuthenticationProvider) hmapClass.newInstance();
    } catch (InstantiationException e) {
      throw new RuntimeException("Whoops, could not create an Authenticator of class " +
          hmapClass.getName());
    } catch (IllegalAccessException e) {
      throw new RuntimeException("Whoops, could not create an Authenticator of class " +
          hmapClass.getName());
    }

    hmap.setConf(config);
  }

  @Override
  public void setMetaStoreHandler(HMSHandler handler) {
    hmap.setMetaStoreHandler(handler);
  }

  @Override
  public void destroy() throws HiveException {
    hmap.destroy();
  }

}
