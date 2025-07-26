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

package org.apache.hive.common.util;

import org.jline.reader.Candidate;
import org.jline.reader.Completer;
import org.jline.reader.LineReader;
import org.jline.reader.ParsedLine;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * A matching string Completer based on JLine's StringCompleter
 */
public class MatchingStringsCompleter implements Completer {
  protected SortedSet<String> candidateStrings = new TreeSet<>();

  public MatchingStringsCompleter() {
    // empty
  }

  public MatchingStringsCompleter(String... strings) {
    this(Arrays.asList(strings));
  }

  public MatchingStringsCompleter(Iterable<String> strings) {
    strings.forEach(candidateStrings::add);
  }

  public Collection<String> getStrings() {
    return candidateStrings;
  }

  @Override
  public void complete(LineReader reader, ParsedLine line, List<Candidate> candidates) {
    Objects.requireNonNull(candidates, "candidates must not be null");

    if (line == null) {
      candidateStrings.stream().map(Candidate::new).forEach(candidates::add);
    } else {
      for (String match : this.candidateStrings.tailSet(line.word())) {
        if (!match.startsWith(line.word())) {
          break;
        }
        candidates.add(new Candidate(match));
      }
    }
  }
}
