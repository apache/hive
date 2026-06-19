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

package org.apache.hadoop.hive.ql.udf;

import java.util.UUID;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * UDFUUID.
 *
 */
@Description(name = "uuid",
value = "_FUNC_() - Returns a universally unique identifier (UUID) string.",
extended = "The value is returned as a canonical UUID 36-character string.\n"
+ "Example:\n"
+ "  > SELECT _FUNC_();\n"
+ "  '0baf1f52-53df-487f-8292-99a03716b688'\n"
+ "  > SELECT _FUNC_();\n"
+ "  '36718a53-84f5-45d6-8796-4f79983ad49d'")
@UDFType(deterministic = false)
public class UDFUUID extends UDF {
  private final Text result = new Text();
  /**
   * Returns a universally unique identifier (UUID) string (36 characters).
   *
   * @return Text
   */
  public Text evaluate() {
    result.set(UUID.randomUUID().toString());
    return result;
  }
}
