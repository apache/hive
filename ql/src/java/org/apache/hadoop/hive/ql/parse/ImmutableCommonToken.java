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

package org.apache.hadoop.hive.ql.parse;

import org.antlr.runtime.CharStream;
import org.antlr.runtime.CommonToken;
import org.antlr.runtime.Token;

/**
 * This class is designed to hold "constant" CommonTokens, that have fixed type
 * and text, and everything else equal to zero. They can therefore be reused
 * to save memory. However, to support reuse (canonicalization) we need to
 * implement the proper hashCode() and equals() methods.
 */
class ImmutableCommonToken extends CommonToken {

  private static final String SETTERS_DISABLED = "All setter methods are intentionally disabled";

  private final int hashCode;

  ImmutableCommonToken(int type, String text) {
    super(type, text);
    hashCode = calculateHash();
  }

  private int calculateHash() {
    return type * 31 + (text != null ? text.hashCode() : 0);
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof ImmutableCommonToken)) {
      return false;
    }
    ImmutableCommonToken otherToken = (ImmutableCommonToken) other;
    return type == otherToken.type &&
        ((text == null && otherToken.text == null) ||
          text != null && text.equals(otherToken.text));
  }

  @Override
  public int hashCode() { return hashCode; }

  // All the setter methods are overridden to throw exception, to prevent accidental
  // attempts to modify data fields that should be immutable.

  @Override
  public void setLine(int line) {
    throw new UnsupportedOperationException(SETTERS_DISABLED);
  }

  @Override
  public void setText(String text) {
    throw new UnsupportedOperationException(SETTERS_DISABLED);
  }

  @Override
  public void setCharPositionInLine(int charPositionInLine) {
    throw new UnsupportedOperationException(SETTERS_DISABLED);
  }

  @Override
  public void setChannel(int channel) {
    throw new UnsupportedOperationException(SETTERS_DISABLED);
  }

  @Override
  public void setType(int type) {
    throw new UnsupportedOperationException(SETTERS_DISABLED);
  }

  @Override
  public void setStartIndex(int start) {
    throw new UnsupportedOperationException(SETTERS_DISABLED);
  }

  @Override
  public void setStopIndex(int stop) {
    throw new UnsupportedOperationException(SETTERS_DISABLED);
  }

  @Override
  public void setTokenIndex(int index) {
    throw new UnsupportedOperationException(SETTERS_DISABLED);
  }

  @Override
  public void setInputStream(CharStream input) {
    throw new UnsupportedOperationException(SETTERS_DISABLED);
  }
}
