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
package org.apache.hadoop.hive.llap.cache;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos;
import org.apache.hadoop.hive.llap.io.api.LlapIo;
import org.apache.hadoop.hive.llap.io.api.LlapProxy;
import org.apache.hadoop.hive.registry.RegistryUtilities;
import org.apache.hive.common.util.ShutdownHookManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars;

/**
 * Simple cache hydration strategy which saves the content info of the cache to a file on the local filesystem on
 * shutdown, and loads it when the daemon starts.
 */
public class BasicLlapCacheHydration implements LlapCacheHydration {

  private static final Logger LOG = LoggerFactory.getLogger(BasicLlapCacheHydration.class);

  private Configuration conf;
  private String savePath;
  @VisibleForTesting
  LlapIo llapIo;

  @Override
  public void load() {
    loadCacheContent();
  }

  @Override
  public void save() {
    saveCacheContent();
  }

  @Override
  public void init() {
    ShutdownHookManager.addShutdownHook(() -> save());
    initSavePath();
    if (llapIo == null) {
      llapIo = LlapProxy.getIo();
    }
  }

  @VisibleForTesting
  void initSavePath() {
    if (savePath == null) {
      String dir = HiveConf.getVar(conf, ConfVars.LLAP_CACHE_HYDRATION_SAVE_DIR);
      String name = RegistryUtilities.getCanonicalHostName();
      if (dir != null && name != null) {
        createDirIfNotExists(dir);
        savePath = dir + Path.SEPARATOR + name.hashCode() + ".cache";
      }
    }
  }

  private void createDirIfNotExists(String dir) {
    File directory = new File(dir);
    if (!directory.exists())
      directory.mkdir();
  }

  private void saveCacheContent() {
    if (llapIo != null && savePath != null) {
      LlapDaemonProtocolProtos.CacheEntryList entryList = llapIo.fetchCachedContentInfo();
      if (!entryList.getEntriesList().isEmpty()) {
        try (OutputStream fos = new FileOutputStream(savePath)) {
          entryList.writeTo(fos);
          LOG.debug("Llap cache content info saved: " + savePath);
        } catch (IOException ex) {
          LOG.warn("Couldn't save llap cache content info.", ex);
        }
      }
    }
  }

  private void loadCacheContent() {
    if (llapIo != null && savePath != null) {
      File file = new File(savePath);
      if (file.exists()) {
        try (FileInputStream fis = new FileInputStream(file)) {
          LlapDaemonProtocolProtos.CacheEntryList entryList = LlapDaemonProtocolProtos.CacheEntryList.parseFrom(fis);
          if (!entryList.getEntriesList().isEmpty()) {
            llapIo.loadDataIntoCache(entryList);
            LOG.debug("Llap cache content info loaded: " + savePath);
            file.delete();
          }
        } catch (IOException ex) {
          LOG.warn("Couldn't load llap cache.", ex);
        }
      }
    }
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }
}
