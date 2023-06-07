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
package org.apache.hadoop.hive.metastore.parser;

import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.misc.Interval;
import org.antlr.v4.runtime.misc.ParseCancellationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PartFilterParser {
  private static final Logger LOG = LoggerFactory.getLogger(PartFilterParser.class);

  public static ExpressionTree parseFilter(String filter) throws MetaException {
    LOG.debug("Parsing filter: " + filter);
    CharStream upperCaseCharStream = new UpperCaseCharStream(CharStreams.fromString(filter));
    PartitionFilterLexer lexer = new PartitionFilterLexer(upperCaseCharStream);
    lexer.removeErrorListeners();
    lexer.addErrorListener(ParseErrorListener.INSTANCE);

    CommonTokenStream tokenStream = new CommonTokenStream(lexer);
    PartitionFilterParser parser = new PartitionFilterParser(tokenStream);
    parser.removeErrorListeners();
    parser.addErrorListener(ParseErrorListener.INSTANCE);

    PartFilterVisitor visitor = new PartFilterVisitor();
    try {
      return visitor.visitFilter(parser.filter());
    } catch (ParseCancellationException e) {
      throw new MetaException("Error parsing partition filter: " + e.getMessage());
    }
  }

  /** Case-insensitive ANTLR string stream */
  private static class UpperCaseCharStream implements CharStream {
    private CodePointCharStream wrapped;

    UpperCaseCharStream(CodePointCharStream wrapped) {
      this.wrapped = wrapped;
    }

    @Override
    public String getText(Interval interval) {
      return wrapped.getText(interval);
    }

    @Override
    public void consume() {
      wrapped.consume();
    }

    @Override
    public int LA(int i) {
      int la = wrapped.LA(i);
      if (la == 0 || la == IntStream.EOF) {
        return la;
      }
      return Character.toUpperCase(la);
    }

    @Override
    public int mark() {
      return wrapped.mark();
    }

    @Override
    public void release(int i) {
      wrapped.release(i);
    }

    @Override
    public int index() {
      return wrapped.index();
    }

    @Override
    public void seek(int i) {
      wrapped.seek(i);
    }

    @Override
    public int size() {
      return wrapped.size();
    }

    @Override
    public String getSourceName() {
      return wrapped.getSourceName();
    }
  }

  /**
   * The ParseErrorListener converts parse errors into ParseCancellationException.
   */
  private static class ParseErrorListener extends BaseErrorListener {
    public static final ParseErrorListener INSTANCE = new ParseErrorListener();
    @Override
    public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol,
                            int line, int charPositionInLine, String msg, RecognitionException e)
        throws ParseCancellationException {
      throw new ParseCancellationException("lexer error: " + msg);
    }
  }
}
