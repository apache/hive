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

package org.apache.hive.beeline;

import org.apache.hadoop.hive.common.io.DigestPrintStream;
import org.apache.hadoop.hive.common.io.FetchConverter;
import org.apache.hadoop.hive.common.io.SortAndDigestPrintStream;
import org.apache.hadoop.hive.common.io.SortPrintStream;

import java.io.PrintStream;

/**
 * Class for representing an OutputFile, into which the writes are converted by the existing
 * FetchConverters.
 */
public class ConvertedOutputFile extends OutputFile {
  private final boolean isActiveFetchConverter;

  public ConvertedOutputFile(OutputFile inner, Converter converter) throws Exception {
    super(converter.getConvertedPrintStream(inner.getOut()), inner.getFilename());
    isActiveFetchConverter = (getOut() instanceof FetchConverter);
  }

  @Override
  boolean isActiveConverter() {
    return isActiveFetchConverter;
  }

  @Override
  void fetchStarted() {
    if (isActiveFetchConverter) {
      ((FetchConverter) getOut()).fetchStarted();
    }
  }

  @Override
  void foundQuery(boolean foundQuery) {
    if (isActiveFetchConverter) {
      ((FetchConverter) getOut()).foundQuery(foundQuery);
    }
  }

  @Override
  void fetchFinished() {
    if (isActiveFetchConverter) {
      ((FetchConverter) getOut()).fetchFinished();
    }
  }

  /**
   * The supported type of converters pointing to a specific FetchConverter class, and the method
   * which provides the actual converted stream.
   */
  public enum Converter {
    SORT_QUERY_RESULTS {
      public PrintStream getConvertedPrintStream(PrintStream inner) throws Exception {
        return new SortPrintStream(inner, "UTF-8");
      }
    },
    HASH_QUERY_RESULTS {
      public PrintStream getConvertedPrintStream(PrintStream inner) throws Exception {
        return new DigestPrintStream(inner, "UTF-8");
      }
    },
    SORT_AND_HASH_QUERY_RESULTS {
      public PrintStream getConvertedPrintStream(PrintStream inner) throws Exception {
        return new SortAndDigestPrintStream(inner, "UTF-8");
      }
    },
    NONE {
      public PrintStream getConvertedPrintStream(PrintStream inner) throws Exception {
        return inner;
      }
    };

    public abstract PrintStream getConvertedPrintStream(PrintStream inner) throws Exception;
  }
}
