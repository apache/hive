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

package org.apache.hadoop.hive.common;

/**
 * Models the list of transactions that should be included in a snapshot.
 * It is modelled as a high water mark, which is the largest write id that
 * has been committed and a list of write ids that are not included.
 */
public interface ValidWriteIdList {

  /**
   * Key used to store valid write id list in a
   * {@link org.apache.hadoop.conf.Configuration} object.
   */
  String VALID_WRITEIDS_KEY = "hive.txn.valid.writeids";

  /**
   * The response to a range query.  NONE means no values in this range match,
   * SOME mean that some do, and ALL means that every value does.
   */
  enum RangeResponse {NONE, SOME, ALL};

  /**
   * Indicates whether a given write ID is valid. Note that valid may have different meanings
   * for different implementations, as some will only want to see committed transactions and some
   * both committed and aborted.
   * @param writeId write ID of the table
   * @return true if valid, false otherwise
   */
  boolean isWriteIdValid(long writeId);

  /**
   * Returns {@code true} if such base file can be used to materialize the snapshot represented by
   * this {@code ValidWriteIdList}.
   * @param writeId highest write ID in a given base_xxxx file
   * @return true if the base file can be used
   */
  boolean isValidBase(long writeId);

  /**
   * Find out if a range of write ids are valid.  Note that valid may have different meanings
   * for different implementations, as some will only want to see committed transactions and some
   * both committed and aborted.
   * @param minWriteId minimum write ID to look for, inclusive
   * @param maxWriteId maximum write ID to look for, inclusive
   * @return Indicate whether none, some, or all of these transactions are valid.
   */
  RangeResponse isWriteIdRangeValid(long minWriteId, long maxWriteId);

  /**
   * Write this ValidWriteIdList into a string. This should produce a string that
   * can be used by {@link #readFromString(String)} to populate a ValidWriteIdList.
   * @return the list as a string
   */
  String writeToString();

  /**
   * Populate this ValidWriteIdList from the string.  It is assumed that the string
   * was created via {@link #writeToString()} and the exceptions list is sorted.
   * @param src source string.
   */
  void readFromString(String src);

  /**
   * Get the table for which the ValidWriteIdList is formed
   * @return table name (&lt;db_name&gt;.&lt;table_name&gt;) associated with ValidWriteIdList.
   */
  String getTableName();

  /**
   * Get the largest write id used.
   * @return largest write id used
   */
  long getHighWatermark();

  /**
   * Get the list of write ids under the high water mark that are not valid.  Note that invalid
   * may have different meanings for different implementations, as some will only want to see open
   * transactions and some both open and aborted.
   * @return a list of invalid write ids
   */
  long[] getInvalidWriteIds();

  /**
   * Indicates whether a given write maps to aborted transaction.
   * @param writeId write id to be validated
   * @return true if aborted, false otherwise
   */
  boolean isWriteIdAborted(long writeId);

  /**
   * Find out if a range of write ids are aborted.
   * @param minWriteId minimum write Id to look for, inclusive
   * @param maxWriteId maximum write Id  to look for, inclusive
   * @return Indicate whether none, some, or all of these write ids are aborted.
   */
  RangeResponse isWriteIdRangeAborted(long minWriteId, long maxWriteId);

  /**
   * The smallest open write id.
   * @return smallest Open write Id in this set, {@code null} if there is none.
   */
  Long getMinOpenWriteId();
}
