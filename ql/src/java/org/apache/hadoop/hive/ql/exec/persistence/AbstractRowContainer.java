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

package org.apache.hadoop.hive.ql.exec.persistence;

import org.apache.hadoop.hive.ql.metadata.HiveException;

public interface AbstractRowContainer<ROW> {

  public interface RowIterator<ROW> {
    public ROW first() throws HiveException;
    public ROW next() throws HiveException;
  }

  public RowIterator<ROW> rowIter() throws HiveException;

  /**
   * add a row into the RowContainer
   *
   * @param t row
   */
  public void addRow(ROW t) throws HiveException;

  /**
   * @return whether the row container has at least 1 row.
   * NOTE: Originally we named this isEmpty, but that name conflicted with another interface.
   */
  public boolean hasRows() throws HiveException;

  /**
   * @return whether the row container has 1 row.
   */
  public boolean isSingleRow() throws HiveException;

  /**
   * @return number of elements in the RowContainer
   */
  public int rowCount() throws HiveException;

  /**
   * Remove all elements in the RowContainer.
   */
  public void clearRows() throws HiveException;
}
