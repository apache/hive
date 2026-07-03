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

package org.apache.hadoop.hive.ql.anon.udf;

import org.apache.hadoop.hive.ql.anon.convert.JsonBodyConverter;
import org.apache.hadoop.hive.ql.anon.model.Msg3;
import org.apache.hadoop.hive.ql.anon.utils.MessageUtils;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

@Description(name = "gen_anon_body",
    value = "_FUNC_(userId) - generate a JSON-serialised Msg3 body for the given userId",
    extended = "Wraps the anon BodyGenerator + JsonBodyConverter so the synthetic "
             + "PII-shaped messages used in the baseline-comparison harness "
             + "can be materialised by a single Hive INSERT.")
public class UDFGenAnonBody extends UDF {

  private static final int DEFAULT_FIELD_LEN = 20;

  private final Text result = new Text();

  public Text evaluate(final IntWritable userId) {
    if (userId == null) {
      return null;
    }
    final Msg3 msg = MessageUtils.createMsg3(userId.get(), DEFAULT_FIELD_LEN);
    result.set(JsonBodyConverter.serializeMsg(msg));
    return result;
  }
}
