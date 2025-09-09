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
package org.apache.hadoop.hive.ql.udf.generic;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.metadata.HiveException;

@Description(
    name = "try_variant_get",
    value = "_FUNC_(variant, path[, type]) - Extracts a sub-variant from variant according to path, and casts it to type. Returns null on error.",
    extended = """
        Example:
        > SELECT _FUNC_(parse_json('{"a": 1}'), '$.a', 'int');
        1
        > SELECT _FUNC_(parse_json('[1, "hello"]'), '$[1]', 'int');
        NULL"""
)
public class GenericUDFTryVariantGet extends GenericUDFVariantGet {

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        try {
            return super.evaluate(arguments);
        } catch (Exception e) {
            return null; // try_variant_get returns null on errors instead of throwing
        }
    }

    @Override
    public String getDisplayString(String[] children) {
        return "try_variant_get(" + String.join(", ", children) + ")";
    }
}