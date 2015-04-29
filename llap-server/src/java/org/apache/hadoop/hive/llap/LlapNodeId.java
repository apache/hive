/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.hive.llap;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

public class LlapNodeId {

  private static final LoadingCache<LlapNodeId, LlapNodeId> CACHE =
      CacheBuilder.newBuilder().softValues().build(
          new CacheLoader<LlapNodeId, LlapNodeId>() {
            @Override
            public LlapNodeId load(LlapNodeId key) throws Exception {
              return key;
            }
          });

  public static LlapNodeId getInstance(String hostname, int port) {
    return CACHE.getUnchecked(new LlapNodeId(hostname, port));
  }


  private final String hostname;
  private final int port;


  private LlapNodeId(String hostname, int port) {
    this.hostname = hostname;
    this.port = port;
  }

  public String getHostname() {
    return hostname;
  }

  public int getPort() {
    return port;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    LlapNodeId that = (LlapNodeId) o;

    if (port != that.port) {
      return false;
    }
    if (!hostname.equals(that.hostname)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = hostname.hashCode();
    result = 1009 * result + port;
    return result;
  }

  @Override
  public String toString() {
    return hostname + ":" + port;
  }
}
