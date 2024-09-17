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
package org.apache.hadoop.hive.llap;

import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;

public interface LlapUgiFactory {
  /*
   * Creates a UserGroupInformation instance for a user.
   * Subclasses might implement a cache for taking care of reusing an existing ugi for the same query if possible.
   */
  UserGroupInformation createUgi(String user) throws IOException;
}
