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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.exec.persistence;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluator;
import org.apache.hadoop.hive.ql.exec.JoinUtil;
import org.apache.hadoop.hive.ql.exec.vector.VectorHashKeyWrapper;
import org.apache.hadoop.hive.ql.exec.vector.VectorHashKeyWrapperBatch;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpressionWriter;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.Writable;

public interface MapJoinTableContainer {
  /**
   * Retrieve rows from hashtable key by key, one key at a time, w/o copying the structures
   * for each key. "Old" HashMapWrapper will still create/retrieve new objects for java HashMap;
   * but the optimized one doesn't have to.
   */
  public interface ReusableGetAdaptor {
    /**
     * Changes current rows to which adaptor is referring to the rows corresponding to
     * the key represented by a VHKW object, and writers and batch used to interpret it.
     */
    JoinUtil.JoinResult setFromVector(VectorHashKeyWrapper kw, VectorExpressionWriter[] keyOutputWriters,
        VectorHashKeyWrapperBatch keyWrapperBatch) throws HiveException;

    /**
     * Changes current rows to which adaptor is referring to the rows corresponding to
     * the key represented by a row object, and fields and ois used to interpret it.
     */
    JoinUtil.JoinResult setFromRow(Object row, List<ExprNodeEvaluator> fields, List<ObjectInspector> ois)
        throws HiveException;

    /**
     * Changes current rows to which adaptor is referring to the rows corresponding to
     * the key that another adaptor has already deserialized via setFromVector/setFromRow.
     */
    JoinUtil.JoinResult setFromOther(ReusableGetAdaptor other) throws HiveException;

    /**
     * Checks whether the current key has any nulls.
     */
    boolean hasAnyNulls(int fieldCount, boolean[] nullsafes);

    /**
     * @return The container w/the rows corresponding to a key set via a previous set... call.
     */
    MapJoinRowContainer getCurrentRows();

    /**
     * @return key
     */
    Object[] getCurrentKey();
  }

  /**
   * Adds row from input to the table.
   */
  MapJoinKey putRow(Writable currentKey, Writable currentValue)
      throws SerDeException, HiveException, IOException;

  /**
   * Indicates to the container that the puts have ended; table is now r/o.
   */
  void seal();

  /**
   * Creates reusable get adaptor that can be used to retrieve rows from the table
   * based on either vectorized or non-vectorized input rows to MapJoinOperator.
   * @param keyTypeFromLoader Last key from hash table loader, to determine key type used
   *                          when loading hashtable (if it can vary).
   */
  ReusableGetAdaptor createGetter(MapJoinKey keyTypeFromLoader);

  /** Clears the contents of the table. */
  void clear();

  MapJoinKey getAnyKey();

  void dumpMetrics();

  /**
   * Checks if the container has spilled any data onto disk.
   * This is only applicable for HybridHashTableContainer.
   */
  boolean hasSpill();

  /**
   * Return the size of the hash table
   */
  int size();

  void setSerde(MapJoinObjectSerDeContext keyCtx, MapJoinObjectSerDeContext valCtx)
      throws SerDeException;
}
