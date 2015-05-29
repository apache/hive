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
package org.apache.hadoop.hive.ql.io.orc;

import java.io.IOException;
import java.util.List;

/**
 * Factory for creating ORC tree readers. These tree readers can handle type promotions and type
 * conversions.
 */
public class ConversionTreeReaderFactory extends TreeReaderFactory {

  // TODO: This is currently only a place holder for type conversions.

  public static TreeReader createTreeReader(int columnId,
      List<OrcProto.Type> types,
      boolean[] included,
      boolean skipCorrupt
  ) throws IOException {
    return TreeReaderFactory.createTreeReader(columnId, types, included, skipCorrupt);
  }
}
