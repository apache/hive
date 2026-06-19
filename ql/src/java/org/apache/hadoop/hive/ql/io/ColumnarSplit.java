/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.io;

/**
 * Interface when implemented should return the estimated size of columnar projections
 * that will be read from the split. This information will be used by split grouper for better
 * grouping based on the actual data read instead of the complete split length.
 */
public interface ColumnarSplit {

  /**
   * Return the estimation size of the column projections that will be read from this split.
   *
   * @return - estimated column projection size that will be read in bytes
   */
  long getColumnarProjectionSize();
}
