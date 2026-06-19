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

package org.apache.hadoop.hive.ql.anon.hooks;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;

public class TestExtractJsonArrayMerge {

  @Test
  public void mergesNdjsonPartFilesIntoOneJsonArray() throws IOException {
    final Path tmp = Files.createTempDirectory("dae-merge-test");
    final Path staging = Files.createDirectories(tmp.resolve("staging"));

    Files.writeString(staging.resolve("part-00000.json"),
        "{\"userId\":1,\"country\":\"GB\"}\n{\"userId\":1,\"country\":\"DE\"}\n");
    Files.writeString(staging.resolve("part-00001.json"),
        "{\"userId\":1,\"country\":\"FR\"}\n");
    Files.writeString(staging.resolve("part-00002.json"), "");

    final Path outFile = tmp.resolve("subject.json");

    ErasureRunCompletionHook.mergeExtractToJsonArray(new Configuration(),
        staging.toAbsolutePath().toString(), outFile.toAbsolutePath().toString());

    Assertions.assertTrue(Files.isRegularFile(outFile),
        "the merge must write a single output file at the caller path");

    final JsonNode arr = new ObjectMapper().readTree(Files.readString(outFile));
    Assertions.assertTrue(arr.isArray(), "the output must be a JSON array");
    Assertions.assertEquals(3, arr.size(),
        "all three NDJSON entries are collected; the empty part-file contributes nothing");

    final Set<String> countries = new HashSet<>();
    for (final JsonNode e : arr) {
      countries.add(e.get("country").asText());
    }
    Assertions.assertEquals(Set.of("GB", "DE", "FR"), countries,
        "every projected entry survives the consolidation, order-independent across part-files");
  }

  @Test
  public void emptyStagingYieldsAnEmptyArray() throws IOException {
    final Path tmp = Files.createTempDirectory("dae-merge-empty");
    final Path staging = Files.createDirectories(tmp.resolve("staging"));
    final Path outFile = tmp.resolve("subject.json");

    ErasureRunCompletionHook.mergeExtractToJsonArray(new Configuration(),
        staging.toAbsolutePath().toString(), outFile.toAbsolutePath().toString());

    final JsonNode arr = new ObjectMapper().readTree(Files.readString(outFile));
    Assertions.assertTrue(arr.isArray() && arr.isEmpty(),
        "a no-match extract still writes a well-formed empty JSON array");
  }
}
