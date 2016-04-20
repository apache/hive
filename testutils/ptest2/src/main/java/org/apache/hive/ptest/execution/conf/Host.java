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

import java.util.Arrays;

import com.google.common.base.Preconditions;

public class Host {
  private final String name;
  private final String user;
  private final int threads;
  private final String[] localDirectories;

  public Host(String name, String user, String[] localDirectories, Integer threads) {
    super();
    this.name = Preconditions.checkNotNull(name, "hostname");
    this.user = Preconditions.checkNotNull(user, "user");
    this.threads = Preconditions.checkNotNull(threads, "threads");
    this.localDirectories = Preconditions.checkNotNull(localDirectories, "localDirectories");
  }
  public String getName() {
    return name;
  }
  public String getUser() {
    return user;
  }
  public int getThreads() {
    return threads;
  }
  public String[] getLocalDirectories() {
    return localDirectories;
  }
  @Override
  public String toString() {
    return "Host [name=" + name + ", user=" + user + ", threads=" + threads
        + ", localDirectories=" + Arrays.toString(localDirectories) + "]";
  }
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + Arrays.hashCode(localDirectories);
    result = prime * result + ((name == null) ? 0 : name.hashCode());
    result = prime * result + threads;
    result = prime * result + ((user == null) ? 0 : user.hashCode());
    return result;
  }
  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    Host other = (Host) obj;
    if (!Arrays.equals(localDirectories, other.localDirectories))
      return false;
    if (name == null) {
      if (other.name != null)
        return false;
    } else if (!name.equals(other.name))
      return false;
    if (threads != other.threads)
      return false;
    if (user == null) {
      if (other.user != null)
        return false;
    } else if (!user.equals(other.user))
      return false;
    return true;
  }


}
