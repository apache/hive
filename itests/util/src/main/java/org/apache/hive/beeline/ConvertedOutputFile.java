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
import org.apache.hadoop.hive.common.io.FetchCallback;
import org.apache.hadoop.hive.common.io.QTestFetchConverter;
import org.apache.hadoop.hive.common.io.SortAndDigestPrintStream;
import org.apache.hadoop.hive.common.io.SortPrintStream;
import org.apache.hadoop.hive.ql.QOutProcessor;

import java.io.PrintStream;

/**
 * Class for representing an OutputFile, into which the writes are converted by the existing
 * FetchConverters.
 */
public class ConvertedOutputFile extends OutputFile {
  private final boolean hasFetchCallback;

  public ConvertedOutputFile(OutputFile inner, Converter converter) throws Exception {
    this(inner, converter, null, null);
  }

  public ConvertedOutputFile(OutputFile inner, Converter converter, QOutProcessor qOutProcessor,
      QOutProcessor.MaskingFoldState maskingFoldState) throws Exception {
    super(wrapStream(inner.getOut(), converter, qOutProcessor, maskingFoldState), inner.getFilename());
    hasFetchCallback = (getOut() instanceof FetchCallback);
  }

  private static PrintStream wrapStream(PrintStream inner, Converter converter,
      QOutProcessor qOutProcessor, QOutProcessor.MaskingFoldState maskingFoldState) throws Exception {
    if (qOutProcessor == null || maskingFoldState == null) {
      return converter.getConvertedPrintStream(inner);
    }
    // Sort before mask: identical MASK lines must not be created by sorting already-masked rows.
    PrintStream masked = new QTestFetchConverter(inner, false, "UTF-8", line -> {
      if (line.startsWith("Reading log file:")) {
        return null;
      }
      return qOutProcessor.maskAndFoldLine(line, maskingFoldState);
    });
    return converter.getConvertedPrintStream(masked);
  }

  @Override
  boolean isActiveConverter() {
    return hasFetchCallback;
  }

  @Override
  void fetchStarted() {
    if (hasFetchCallback) {
      ((FetchCallback) getOut()).fetchStarted();
    }
  }

  @Override
  void foundQuery(boolean foundQuery) {
    if (hasFetchCallback) {
      ((FetchCallback) getOut()).foundQuery(foundQuery);
    }
  }

  @Override
  void fetchFinished() {
    if (hasFetchCallback) {
      ((FetchCallback) getOut()).fetchFinished();
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
