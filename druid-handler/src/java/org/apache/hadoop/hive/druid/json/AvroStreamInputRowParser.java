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
package org.apache.hadoop.hive.druid.json;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.data.input.ByteBufferInputRowParser;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.impl.ParseSpec;

import javax.validation.constraints.NotNull;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;

/**
 * This class is copied from druid source code
 * in order to avoid adding additional dependencies on druid-indexing-service.
 */
public class AvroStreamInputRowParser implements ByteBufferInputRowParser {
  private final ParseSpec parseSpec;
  private final AvroBytesDecoder avroBytesDecoder;

  @JsonCreator public AvroStreamInputRowParser(@JsonProperty("parseSpec") ParseSpec parseSpec,
      @JsonProperty("avroBytesDecoder") AvroBytesDecoder avroBytesDecoder) {
    this.parseSpec = Preconditions.checkNotNull(parseSpec, "parseSpec");
    this.avroBytesDecoder = Preconditions.checkNotNull(avroBytesDecoder, "avroBytesDecoder");
  }

  @NotNull @Override public List<InputRow> parseBatch(ByteBuffer input) {
    throw new UnsupportedOperationException("This class is only used for JSON serde");
  }

  @JsonProperty @Override public ParseSpec getParseSpec() {
    return parseSpec;
  }

  @JsonProperty public AvroBytesDecoder getAvroBytesDecoder() {
    return avroBytesDecoder;
  }

  @Override public ByteBufferInputRowParser withParseSpec(ParseSpec parseSpec) {
    return new AvroStreamInputRowParser(parseSpec, avroBytesDecoder);
  }

  @Override public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final AvroStreamInputRowParser that = (AvroStreamInputRowParser) o;
    return Objects.equals(parseSpec, that.parseSpec) && Objects.equals(avroBytesDecoder, that.avroBytesDecoder);
  }

  @Override public int hashCode() {
    return Objects.hash(parseSpec, avroBytesDecoder);
  }
}
