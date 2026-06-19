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
package org.apache.hadoop.hive.ql.parse.repl;

import org.apache.hadoop.hive.ql.parse.repl.load.log.state.DataCopyEnd;
import org.apache.hadoop.hive.ql.exec.repl.util.ReplUtils;
import org.reflections.Reflections;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.lang.reflect.Field;
import java.time.Instant;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class TestReplStateLogTimeFormat {
  private static final long randomDate = 1659367078L;
  private static final String UTCString = Instant.ofEpochSecond(randomDate).toString();
  private static final Set<Class<? extends ReplState>> DUMP_LOG_EXEMPTED_CLASSES;
  private static final Set<Class<? extends ReplState>> LOAD_LOG_EXEMPTED_CLASSES;

  static {
    // Add classes which don't have a time field or time fields which need not be serialized to UTC format.
    DUMP_LOG_EXEMPTED_CLASSES = new HashSet<>();

    LOAD_LOG_EXEMPTED_CLASSES = new HashSet<>();
    LOAD_LOG_EXEMPTED_CLASSES.add(DataCopyEnd.class);
  }

  private void verifyAnnotationAndSetEpoch(ReplState replState) throws Exception {
    boolean isAnnotationSet = false;
    for (Field field : replState.getClass().getDeclaredFields()) {
      if (Objects.nonNull(field.getAnnotation(JsonSerialize.class))) {
        if (ReplUtils.TimeSerializer.class.equals(field.getAnnotation(JsonSerialize.class).using())) {
          field.setAccessible(true);
          field.set(replState, randomDate);
          isAnnotationSet = true;
        }
      }
    }
    assertTrue(
      String.format(
        "Class %s has a time field which is not annotated with @JsonSerialize(using = ReplUtils.TimeSerializer.class) " +
        "Please annotate the time field with it or add it to appropriate exempted set above",
        replState.getClass().getName()),
        isAnnotationSet
      );
  }

  private void verifyUTCString(ReplState replState) throws Exception {
    ObjectMapper objectMapper = new ObjectMapper();
    String json = objectMapper.writeValueAsString(replState);
    assertTrue(
      String.format(
        "Expected UTC string %s not found in serialized representation of %s",
        UTCString, replState.getClass().getName()),
      json.contains(UTCString)
    );
  }


  private void verifyTimeFormat(String packagePath, Set<Class<? extends ReplState>> EXEMPTED_CLASS) throws Exception {
    Set<Class<? extends ReplState>> replStateLogClasses
            = new Reflections(packagePath).getSubTypesOf(ReplState.class);
    for (Class<? extends ReplState> cls : replStateLogClasses) {
      if (EXEMPTED_CLASS.contains(cls)) {
        continue;
      }
      ReplState replState = mock(cls);
      verifyAnnotationAndSetEpoch(replState);
      verifyUTCString(replState);
    }
  }

  @Test
  public void testReplLogTimeFormat() throws Exception {
    verifyTimeFormat("org.apache.hadoop.hive.ql.parse.repl.dump.log.state", DUMP_LOG_EXEMPTED_CLASSES);
    verifyTimeFormat("org.apache.hadoop.hive.ql.parse.repl.load.log.state", LOAD_LOG_EXEMPTED_CLASSES);
  }

  @Test(expected = AssertionError.class)
  public void testClassWithoutAnnotation() throws Exception {
    ReplState replStateClassWithoutAnnotation = new ReplState() {
      private Long StartTime;
    };
    verifyAnnotationAndSetEpoch(replStateClassWithoutAnnotation);
  }

}
