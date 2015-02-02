/**
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

package org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang.ObjectUtils;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.io.Text;

public class LazyObjectInspectorParametersImpl implements
    LazyObjectInspectorParameters {
  protected boolean escaped;
  protected byte escapeChar;
  protected boolean extendedBooleanLiteral;
  protected List<String> timestampFormats;
  protected byte[] separators;
  protected Text nullSequence;
  protected boolean lastColumnTakesRest;


  public LazyObjectInspectorParametersImpl() {
    this.escaped = false;
    this.extendedBooleanLiteral = false;
    this.timestampFormats = null;
  }

  public LazyObjectInspectorParametersImpl(boolean escaped, byte escapeChar,
      boolean extendedBooleanLiteral, List<String> timestampFormats,
      byte[] separators, Text nullSequence) {
    super();
    this.escaped = escaped;
    this.escapeChar = escapeChar;
    this.extendedBooleanLiteral = extendedBooleanLiteral;
    this.timestampFormats = timestampFormats;
    this.separators = separators;
    this.nullSequence = nullSequence;
    this.lastColumnTakesRest = false;
  }

  public LazyObjectInspectorParametersImpl(boolean escaped, byte escapeChar,
      boolean extendedBooleanLiteral, List<String> timestampFormats,
      byte[] separators, Text nullSequence, boolean lastColumnTakesRest) {
    super();
    this.escaped = escaped;
    this.escapeChar = escapeChar;
    this.extendedBooleanLiteral = extendedBooleanLiteral;
    this.timestampFormats = timestampFormats;
    this.separators = separators;
    this.nullSequence = nullSequence;
    this.lastColumnTakesRest = lastColumnTakesRest;
  }

  public LazyObjectInspectorParametersImpl(LazyObjectInspectorParameters lazyParams) {
    this.escaped = lazyParams.isEscaped();
    this.escapeChar = lazyParams.getEscapeChar();
    this.extendedBooleanLiteral = lazyParams.isExtendedBooleanLiteral();
    this.timestampFormats = lazyParams.getTimestampFormats();
    this.separators = lazyParams.getSeparators();
    this.nullSequence = lazyParams.getNullSequence();
    this.lastColumnTakesRest = lazyParams.isLastColumnTakesRest();
  }

  @Override
  public boolean isEscaped() {
    return escaped;
  }

  @Override
  public byte getEscapeChar() {
    return escapeChar;
  }

  @Override
  public boolean isExtendedBooleanLiteral() {
    return extendedBooleanLiteral;
  }

  @Override
  public List<String> getTimestampFormats() {
    return timestampFormats;
  }

  @Override
  public byte[] getSeparators() {
    return separators;
  }

  @Override
  public Text getNullSequence() {
    return nullSequence;
  }

  @Override
  public boolean isLastColumnTakesRest() {
    return lastColumnTakesRest;
  }

  protected boolean equals(LazyObjectInspectorParametersImpl other) {
    return this.escaped == other.escaped
        && this.escapeChar == other.escapeChar
        && this.extendedBooleanLiteral == other.extendedBooleanLiteral
        && this.lastColumnTakesRest == other.lastColumnTakesRest
        && ObjectUtils.equals(this.nullSequence, other.nullSequence)
        && Arrays.equals(this.separators, other.separators)
        && ObjectUtils.equals(this.timestampFormats, other.timestampFormats);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof LazyObjectInspectorParametersImpl)) {
      return false;
    }
    return equals((LazyObjectInspectorParametersImpl) obj);
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder().append(escaped).append(escapeChar)
        .append(extendedBooleanLiteral).append(timestampFormats)
        .append(lastColumnTakesRest).append(nullSequence).append(separators).toHashCode();
  }
}
