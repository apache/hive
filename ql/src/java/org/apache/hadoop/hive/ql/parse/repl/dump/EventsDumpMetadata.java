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

package org.apache.hadoop.hive.ql.parse.repl.dump;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.exec.util.Retryable;
import org.apache.hadoop.hive.ql.metadata.HiveException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;

/**
 * This class represents the metadata related to events in repl dump.
 * Metadata includes,
 * lastReplId - the last successfully dumped eventId.
 * eventsDumpedCount - number of events dumped in staging directory.
 * eventsBatched - denotes if events are dumped in batches.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class EventsDumpMetadata {
  @JsonProperty
  private Long lastReplId;
  @JsonProperty
  private Integer eventsDumpedCount;
  @JsonProperty
  private boolean eventsBatched;
  private static final ObjectMapper objectMapper = new ObjectMapper();

  public EventsDumpMetadata() {

  }

  public EventsDumpMetadata(Long lastReplId, Integer eventsDumpedCount, boolean eventsBatched) {
    this.lastReplId = lastReplId;
    this.eventsDumpedCount = eventsDumpedCount;
    this.eventsBatched = eventsBatched;
  }

  public Long getLastReplId() {
    return lastReplId;
  }

  public Integer getEventsDumpedCount() {
    return eventsDumpedCount;
  }

  public void setLastReplId(Long lastReplId) {
    this.lastReplId = lastReplId;
  }

  public void setEventsDumpedCount(Integer eventsDumpedCount) {
    this.eventsDumpedCount = eventsDumpedCount;
  }

  public void incrementEventsDumpedCount() {
    this.eventsDumpedCount++;
  }

  public boolean isEventsBatched() {
    return eventsBatched;
  }

  public void setEventsBatched(boolean eventsBatched) {
    this.eventsBatched = eventsBatched;
  }

  public String serialize() throws JsonProcessingException {
    return objectMapper.writeValueAsString(this);
  }

  public static EventsDumpMetadata deserialize(Path ackFile, HiveConf conf) throws HiveException {
    Retryable retryable = Retryable.builder()
            .withHiveConf(conf)
            .withRetryOnException(IOException.class).build();
    try {
      return retryable.executeCallable(() -> {
        FileSystem fs = ackFile.getFileSystem(conf);
        try (BufferedReader br = new BufferedReader(
                new InputStreamReader(fs.open(ackFile), Charset.defaultCharset()))
        ) {
          return objectMapper.readValue(br, EventsDumpMetadata.class);
        }
      });
    } catch (Exception e) {
      throw new HiveException(ErrorMsg.REPL_RETRY_EXHAUSTED.format(e.getMessage()), e);
    }
  }
}
