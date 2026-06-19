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
package org.apache.hive.hcatalog.data;

import java.util.List;

import org.apache.hadoop.io.Writable;

/**
 * Interface that determines whether we can implement a HCatRecord on top of it
 */
public interface HCatRecordable extends Writable {

  /**
   * Gets the field at the specified index.
   * @param fieldNum the field number
   * @return the object at the specified index
   */
  Object get(int fieldNum);

  /**
   * Gets all the fields of the hcat record.
   * @return the list of fields
   */
  List<Object> getAll();

  /**
   * Sets the field at the specified index.
   * @param fieldNum the field number
   * @param value the value to set
   */
  void set(int fieldNum, Object value);

  /**
   * Gets the size of the hcat record.
   * @return the size
   */
  int size();

}
