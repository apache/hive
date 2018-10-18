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

package org.apache.hadoop.hive.ql.io;

import java.util.Set;

import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;

import com.google.common.collect.ImmutableSet;

public class ORCFileStorageFormatDescriptor extends AbstractStorageFormatDescriptor {
  @Override
  public Set<String> getNames() {
    return ImmutableSet.of(IOConstants.ORCFILE, IOConstants.ORC);
  }
  @Override
  public String getInputFormat() {
    return OrcInputFormat.class.getName();
  }
  @Override
  public String getOutputFormat() {
    return OrcOutputFormat.class.getName();
  }
  @Override
  public String getSerde() {
    return OrcSerde.class.getName();
  }
}
