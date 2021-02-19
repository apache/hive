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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class TestMemoryLimitedPathCache {

  @Test
  public void testCache() {
    Configuration conf = new Configuration();
    HiveConf.setVar(conf, HiveConf.ConfVars.LLAP_IO_PATH_CACHE_SIZE, "10Mb");
    PathCache pathCache = new MemoryLimitedPathCache(conf);

    Long[] keys = { 0L, 1L, 2L, 3L, 4L };
    String[] values = { "test_0", "test_1", "test_2", "test_3", "test_4" };

    pathCache.touch(keys[0], values[0]);
    pathCache.touch(keys[1], values[1]);
    pathCache.touch(keys[2], values[2]);
    pathCache.touch(keys[3], values[3]);
    pathCache.touch(keys[4], values[4]);

    assertEquals(values[4], pathCache.resolve(keys[4]));
    assertEquals(values[3], pathCache.resolve(keys[3]));
    assertEquals(values[0], pathCache.resolve(keys[0]));
    assertEquals(values[1], pathCache.resolve(keys[1]));
    assertEquals(values[2], pathCache.resolve(keys[2]));
  }

  @Test
  public void testMemoryLimit() {
    Configuration conf = new Configuration();
    HiveConf.setVar(conf, HiveConf.ConfVars.LLAP_IO_PATH_CACHE_SIZE, "400");
    PathCache pathCache = new MemoryLimitedPathCache(conf);

    Long key = 1L;
    String value = "36_36_36_36_36_36_36_36_36_36_36_36_";

    pathCache.touch(key, value);
    String resolve = pathCache.resolve(key);
    assertEquals(value, resolve);

    pathCache.touch(2, value);
    pathCache.touch(3, value);
    pathCache.touch(4, value);
    pathCache.touch(5, value);


    resolve = pathCache.resolve(key);
    assertNull(resolve);
  }
}
