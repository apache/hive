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

/**
 * Cache for storing the file paths for a given file key.
 */
public interface PathCache {

  /**
   * Puts the value in the cache if its not there.
   * Updates the value for a given key.
   * Updates the access time for a given key.
   */
  void touch(Object key, String value);

  /**
   * Returns the value for the given file key, or null if its not found.
   */
  String resolve(Object key);

}
