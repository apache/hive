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

package org.apache.hadoop.hive.llap.io.encoded;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.hive.ql.io.orc.StripeInformation;
import org.apache.hadoop.hive.ql.io.orc.OrcProto.Type;

/** ORC-specific metadata cache. */
public class OrcMetadataCache {
  private final ConcurrentHashMap<String, List<StripeInformation>> metadataCacheStripes =
      new ConcurrentHashMap<String, List<StripeInformation>>();
  private final ConcurrentHashMap<String, List<Type>> metadataCacheTypes =
      new ConcurrentHashMap<String, List<Type>>();

  public List<StripeInformation> getStripes(String internedFilePath) {
    return metadataCacheStripes.get(internedFilePath);
  }

  public List<Type> getTypes(String internedFilePath) {
    return metadataCacheTypes.get(internedFilePath);
  }

  public void cacheStripes(String internedFilePath, List<StripeInformation> stripes) {
    metadataCacheStripes.putIfAbsent(internedFilePath, stripes);
  }

  public void cacheTypes(String internedFilePath, List<Type> types) {
    metadataCacheTypes.putIfAbsent(internedFilePath, types);
  }
}
