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

package org.apache.hadoop.hive.llap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.llap.daemon.impl.QueryIdentifier;
import org.apache.hadoop.hive.llap.security.LlapUgiHelper;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LlapUgiManager {
  private static final Logger LOG = LoggerFactory.getLogger(LlapUgiManager.class);
  private final ConcurrentMap<QueryIdentifier, UserGroupInformation> ugis = new ConcurrentHashMap<>();
  private final LlapUgiFactory ugiFactory;

  private LlapUgiManager(Configuration conf) {
    try {
      ugiFactory = LlapUgiHelper.createLlapUgiFactory(conf);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static LlapUgiManager getInstance(Configuration daemonConf) {
    return new LlapUgiManager(daemonConf);
  }

  /**
   * Creates (or returns) an ugi for tasks in the same query and merges the credentials.
   * This is valid to be done once per query: no vertex-level ugi and credentials are needed, both of them
   * are the same within the same query.
   * Regarding vertex user: LlapTaskCommunicator has a single "user" field,
   * which is passed into the SignableVertexSpec.
   * Regarding credentials: LlapTaskCommunicator creates SubmitWorkRequestProto instances,
   * into which dag-level credentials are passed.
   * The most performant way would be to use a single UGI for the same user in the daemon, but that's not possible,
   * because the credentials can theoretically change across queries.
   */
  public UserGroupInformation getOrCreateUgi(QueryIdentifier queryIdentifier, String user, Credentials credentials) {
    return ugis.computeIfAbsent(queryIdentifier,
        k -> {
          try {
            UserGroupInformation ugi = ugiFactory.createUgi(user);
            ugi.addCredentials(credentials);
            LOG.info("Created ugi {} for queryIdentifier '{}', current ugis #: {}", ugi, queryIdentifier, ugis.size());
            return ugi;
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        });
  }

  /*
   * Closes all filesystems for a specific query.
   * In LLAP daemons for the same dag user and credentials, this call typically closes a single FileSystem instance.
   */
  public void closeAllForUgi(QueryIdentifier queryIdentifier) {
    LOG.debug("Closing all FileSystems for queryIdentifier '{}'", queryIdentifier);
    try {
      FileSystem.closeAllForUGI(ugis.get(queryIdentifier));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    ugis.remove(queryIdentifier);
  }
}
