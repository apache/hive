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

package org.apache.hadoop.hive.ql.anon.gen;

import org.apache.avro.specific.SpecificRecordBase;
import org.apache.hadoop.hive.metastore.api.ColumnInternalFormat;
import org.apache.hadoop.hive.ql.anon.convert.BodyConverter;
import org.apache.hadoop.hive.ql.anon.convert.BodyConverterFactory;
import org.apache.hadoop.hive.ql.anon.utils.AvroUtils;
import org.apache.hadoop.io.Writable;

public class AvroGenerator implements BodyGenerator {

  private final BodyConverter converter;

  public AvroGenerator(final ColumnInternalFormat internalFormat) {
    this.converter = BodyConverterFactory.getBodyConverter(internalFormat);
  }

  @Override
  public Writable generateMsg1(long txnId) {
    final SpecificRecordBase msg = AvroUtils.createMsg1(txnId);
    return converter.convertMessage(msg);
  }

  @Override
  public Writable generateMsg2(int userId) {
    final SpecificRecordBase msg = AvroUtils.createMsg2(userId);
    return converter.convertMessage(msg);
  }

  @Override
  public Writable generateMsg3(final int userId, final int len) {
    final SpecificRecordBase msg = AvroUtils.createMsg3(userId, len);
    return converter.convertMessage(msg);
  }
}
