/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hive.ptest.execution;

import java.io.File;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class LogDirectoryCleaner extends Thread {
  private static final Logger LOG = LoggerFactory
      .getLogger(LogDirectoryCleaner.class);
  private final File mLogDir;
  private final int mMaxDirectoriesPerProfile;
  public LogDirectoryCleaner(File logDir, int maxDirectoriesPerProfile) {
    mLogDir = logDir;
    mMaxDirectoriesPerProfile = maxDirectoriesPerProfile;
  }

  @Override
  public void run() {
    try {
      File[] logDirs = mLogDir.listFiles();
      if(logDirs != null &&  logDirs.length > 0) {
        Map<String, ProfileLogs> profiles = Maps.newHashMap();
        for(File logDir : logDirs) {
          String name = logDir.getName();
          if(name.contains("-")) {
            String profile = name.substring(0, name.lastIndexOf("-"));
            ProfileLogs logs = profiles.get(profile);
            if(logs == null) {
              logs = new ProfileLogs(profile);
              profiles.put(profile, logs);
            }
            logs.dirs.add(logDir);
          }
        }
        for(String profile : profiles.keySet()) {
          ProfileLogs logs = profiles.get(profile);
          if(logs.dirs.size() > mMaxDirectoriesPerProfile) {
            File oldest = logs.getOldest();
            LOG.info("Deleting " + oldest + " from " + logs.dirs);
            FileUtils.deleteQuietly(oldest);
          }
        }
      }
    } catch(Throwable t) {
      LOG.error("Unexpected error cleaning " + mLogDir, t);
    }
  }

  private static class ProfileLogs {
    String name;
    List<File> dirs = Lists.newArrayList();
    ProfileLogs(String name) {
      this.name = name;
    }
    File getOldest() {
      Preconditions.checkState(!dirs.isEmpty(), "Cannot be called unless dirs.size() >= 1");
      File eldestDir = null;
      long eldestId = Long.MAX_VALUE;
      for(File dir : dirs) {
        try {
          long id = Long.parseLong(dir.getName().substring(name.length() + 1));
          if(id < eldestId) {
            eldestId = id;
            eldestDir = dir;
          }
        } catch (NumberFormatException e) {
          LOG.warn("Error parsing " + dir.getName(), e);
        }
      }
      return Preconditions.checkNotNull(eldestDir, "eldestDir");
    }
  }
}
