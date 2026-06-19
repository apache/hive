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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hive.hcatalog.api.repl;

/**
 * Interface for a client to provide a Staging Directory specification
 */
public interface StagingDirectoryProvider {

  /**
   * Return a temporary staging directory for a given key
   * @param key key for the directory, usually a name of a partition
   * Note that when overriding this method, no guarantees are made about the
   * contents of the key, other than that is unique per partition.
   * @return A parth specification to use as a temporary staging directory
   */
  String getStagingDirectory(String key);

  /**
   * Trivial implementation of this interface - creates
   */
  public class TrivialImpl implements StagingDirectoryProvider {

    String prefix = null;

    /**
     * Trivial implementation of StagingDirectoryProvider which takes a temporary directory
     * and creates directories inside that for each key. Note that this is intended as a
     * trivial implementation, and if any further "advanced" behaviour is desired,
     * it is better that the user roll their own.
     *
     * @param base temp directory inside which other tmp dirs are created
     * @param separator path separator. Usually should be "/"
     */
    public TrivialImpl(String base,String separator){
      this.prefix = base + separator;
    }

    @Override
    public String getStagingDirectory(String key) {
      return prefix + key;
    }
  }
}
