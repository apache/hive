/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.Weigher;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Memory limited version of the path cache.
 */
public class MemoryLimitedPathCache implements PathCache {

  private static final Logger LOG = LoggerFactory.getLogger(MemoryLimitedPathCache.class);
  private Cache<Object, String> internalCache;

  public MemoryLimitedPathCache(Configuration conf) {
    internalCache = CacheBuilder.newBuilder()
        .maximumWeight(HiveConf.getSizeVar(conf, HiveConf.ConfVars.LLAP_IO_PATH_CACHE_SIZE))
        .weigher(new PathWeigher())
        .build();
  }

  @Override
  public void touch(Object key, String val) {
    if (key != null) {
      internalCache.put(key, val);
    }
  }

  @Override
  public String resolve(Object key) {
    return key != null ? internalCache.getIfPresent(key) : null;
  }

  private static class PathWeigher implements Weigher<Object, String> {

    @Override
    public int weigh(Object key, String value) {
      /*
       String memory footprint approximation. The actual value depends on the implementation so the following
       approximation was used.

       8 object header used by the VM
       8 64-bit reference to char array (value)
       8 + string.length() * 2 character array itself (object header + 16-bit chars)
       4 cached hash code

       (The final value should be multiple of 8 but for the sake of simplicity that's omitted for now.)
      */
      return 2 * value.length() + 28;
    }
  }

}
