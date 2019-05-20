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
package org.apache.hive.ptest.execution.conf;

import java.io.File;
import java.io.IOException;

import org.apache.hive.ptest.execution.Dirs;
import org.apache.hive.ptest.execution.context.ExecutionContextProvider;
import org.apache.hive.ptest.execution.context.FixedExecutionContextProvider;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;

public class ExecutionContextConfiguration {
  public static final String WORKING_DIRECTORY = "workingDirectory";
  public static final String PROFILE_DIRECTORY = "profileDirectory";
  public static final String MAX_LOG_DIRS_PER_PROFILE = "maxLogDirectoriesPerProfile";
  private static final String MAX_RSYNC_THREADS = "maxRsyncThreads";
  private static final int MAX_RSYNC_THREADS_DEFAULT = 10;
  private final ExecutionContextProvider mExecutionContextProvider;
  private final String mWorkingDirectory;
  private final String mGlobalLogDirectory;
  private final String mProfileDirectory;
  private final int mMaxLogDirectoriesPerProfile;
  private final int mMaxRsyncThreads;

  @VisibleForTesting
  public ExecutionContextConfiguration(Context context)
      throws IOException {
    mWorkingDirectory = context.getString(WORKING_DIRECTORY, "").trim();
    Preconditions.checkArgument(!mWorkingDirectory.isEmpty(), WORKING_DIRECTORY + " is required");
    mProfileDirectory = context.getString(PROFILE_DIRECTORY, "").trim();
    Preconditions.checkArgument(!mProfileDirectory.isEmpty(), PROFILE_DIRECTORY + " is required");
    mGlobalLogDirectory = Dirs.create(new File(mWorkingDirectory, "logs")).getAbsolutePath();
    mMaxLogDirectoriesPerProfile = context.getInteger(MAX_LOG_DIRS_PER_PROFILE, 10);
    mMaxRsyncThreads = context.getInteger(MAX_RSYNC_THREADS, MAX_RSYNC_THREADS_DEFAULT);
    String executionContextProviderBuilder = context.getString("executionContextProvider",
        FixedExecutionContextProvider.Builder.class.getName()).trim();
    try {
      Object builder = Class.forName(executionContextProviderBuilder).newInstance();
      if(!(builder instanceof ExecutionContextProvider.Builder)) {
        throw new IllegalArgumentException("executionContextProvider must be of type " +
            ExecutionContextProvider.Builder.class.getName());
      }
      mExecutionContextProvider = ((ExecutionContextProvider.Builder)builder)
          .build(context, mWorkingDirectory);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
  public int getMaxRsyncThreads() {
    return mMaxRsyncThreads;
  }
  public int getMaxLogDirectoriesPerProfile() {
    return mMaxLogDirectoriesPerProfile;
  }
  public String getWorkingDirectory() {
    return mWorkingDirectory;
  }
  public String getGlobalLogDirectory() {
    return mGlobalLogDirectory;
  }
  public String getProfileDirectory() {
    return mProfileDirectory;
  }
  public ExecutionContextProvider getExecutionContextProvider() {
    return mExecutionContextProvider;
  }

  public static ExecutionContextConfiguration withContext(Context ctx)
      throws IOException {
    return new ExecutionContextConfiguration(ctx);
  }
  @Override
  public String toString() {
    return "ExecutionContextConfiguration{" +
        "mExecutionContextProvider=" + mExecutionContextProvider +
        ", mWorkingDirectory='" + mWorkingDirectory + '\'' +
        ", mGlobalLogDirectory='" + mGlobalLogDirectory + '\'' +
        ", mProfileDirectory='" + mProfileDirectory + '\'' +
        ", mMaxLogDirectoriesPerProfile=" + mMaxLogDirectoriesPerProfile +
        ", mMaxRsyncThreads=" + mMaxRsyncThreads +
        '}';
  }
}
