/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.serde2.avro;

import org.apache.avro.Schema;

/**
 * Simple pair class used for memoizing schema-reencoding operations.
 */
class ReaderWriterSchemaPair {
  final Schema reader;
  final Schema writer;

  public ReaderWriterSchemaPair(Schema writer, Schema reader) {
    this.reader = reader;
    this.writer = writer;
  }

  public Schema getReader() {
    return reader;
  }

  public Schema getWriter() {
    return writer;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    ReaderWriterSchemaPair that = (ReaderWriterSchemaPair) o;

    if (!reader.equals(that.reader)) return false;
    if (!writer.equals(that.writer)) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = reader.hashCode();
    result = 31 * result + writer.hashCode();
    return result;
  }
}
