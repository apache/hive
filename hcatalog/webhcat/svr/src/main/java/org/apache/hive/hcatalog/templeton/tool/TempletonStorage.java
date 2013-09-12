/**
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
package org.apache.hive.hcatalog.templeton.tool;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;

/**
 * An interface to handle different Templeton storage methods, including
 * ZooKeeper and HDFS.  Any storage scheme must be able to handle being
 * run in an HDFS environment, where specific file systems and virtual
 * machines may not be available.
 *
 * Storage is done individually in a hierarchy: type (the data type,
 * as listed below), then the id (a given jobid, jobtrackingid, etc.),
 * then the key/value pairs.  So an entry might look like:
 *
 * JOB
 *   jobid00035
 *     user -&gt; rachel
 *     datecreated -&gt; 2/5/12
 *     etc.
 *
 * Each field must be available to be fetched/changed individually.
 */
public interface TempletonStorage {
  // These are the possible types referenced by 'type' below.
  public enum Type {
    UNKNOWN, JOB, JOBTRACKING, TEMPLETONOVERHEAD
  }

  public static final String STORAGE_CLASS    = "templeton.storage.class";
  public static final String STORAGE_ROOT     = "templeton.storage.root";

  /**
   * Start the cleanup process for this storage type.
   * @param config
   */
  public void startCleanup(Configuration config);

  /**
   * Save a single key/value pair for a specific job id.
   * @param type The data type (as listed above)
   * @param id The String id of this data grouping (jobid, etc.)
   * @param key The name of the field to save
   * @param val The value of the field to save
   */
  public void saveField(Type type, String id, String key, String val)
    throws NotFoundException;

  /**
   * Get the value of one field for a given data type.  If the type
   * is UNKNOWN, search for the id in all types.
   * @param type The data type (as listed above)
   * @param id The String id of this data grouping (jobid, etc.)
   * @param key The name of the field to retrieve
   * @return The value of the field requested, or null if not
   * found.
   */
  public String getField(Type type, String id, String key);

  /**
   * Get all the name/value pairs stored for this id.
   * Be careful using getFields() -- optimistic locking will mean that
   * your odds of a conflict are decreased if you read/write one field
   * at a time.  getFields() is intended for read-only usage.
   *
   * If the type is UNKNOWN, search for the id in all types.
   *
   * @param type The data type (as listed above)
   * @param id The String id of this data grouping (jobid, etc.)
   * @return A Map of key/value pairs found for this type/id.
   */
  public Map<String, String> getFields(Type type, String id);

  /**
   * Delete a data grouping (all data for a jobid, all tracking data
   * for a job, etc.).  If the type is UNKNOWN, search for the id
   * in all types.
   *
   * @param type The data type (as listed above)
   * @param id The String id of this data grouping (jobid, etc.)
   * @return True if successful, false if not, throws NotFoundException
   * if the id wasn't found.
   */
  public boolean delete(Type type, String id) throws NotFoundException;

  /**
   * Get the id of each data grouping in the storage system.
   *
   * @return An ArrayList<String> of ids.
   */
  public List<String> getAll();

  /**
   * Get the id of each data grouping of a given type in the storage
   * system.
   * @param type The data type (as listed above)
   * @return An ArrayList<String> of ids.
   */
  public List<String> getAllForType(Type type);

  /**
   * Get the id of each data grouping that has the specific key/value
   * pair.
   * @param key The name of the field to search for
   * @param value The value of the field to search for
   * @return An ArrayList<String> of ids.
   */
  public List<String> getAllForKey(String key, String value);

  /**
   * Get the id of each data grouping of a given type that has the
   * specific key/value pair.
   * @param type The data type (as listed above)
   * @param key The name of the field to search for
   * @param value The value of the field to search for
   * @return An ArrayList<String> of ids.
   */
  public List<String> getAllForTypeAndKey(Type type, String key,
                      String value);

  /**
   * For storage methods that require a connection, this is a hint
   * that it's time to open a connection.
   */
  public void openStorage(Configuration config) throws IOException;

  /**
   * For storage methods that require a connection, this is a hint
   * that it's time to close the connection.
   */
  public void closeStorage() throws IOException;
}
