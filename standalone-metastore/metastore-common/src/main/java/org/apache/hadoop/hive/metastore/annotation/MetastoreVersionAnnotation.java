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

package org.apache.hadoop.hive.metastore.annotation;

import org.apache.hadoop.classification.InterfaceStability;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;


/**
 * HiveVersionAnnotation.
 *
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.PACKAGE)
@InterfaceStability.Unstable
public @interface MetastoreVersionAnnotation {

  /**
   * Get the Hive version
   * @return the version string "0.6.3-dev"
   */
  String version();

  /**
   * Get the Hive short version containing major/minor/change version numbers
   * @return the short version string "0.6.3"
   */
  String shortVersion();

  /**
   * Get the username that compiled Hive.
   */
  String user();

  /**
   * Get the date when Hive was compiled.
   * @return the date in unix 'date' format
   */
  String date();

  /**
   * Get the url for the git repository.
   */
  String url();

  /**
   * Get the git revision.
   * @return the revision number as a string (eg. "451451")
   */
  String revision();

  /**
   * Get the branch from which this was compiled.
   * @return The branch name, e.g. "trunk" or "branches/branch-0.20"
   */
  String branch();

  /**
   * Get a checksum of the source files from which
   * Hive was compiled.
   * @return a string that uniquely identifies the source
   **/
  String srcChecksum();

}
