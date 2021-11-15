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

import java.util.ArrayList;
import org.antlr.runtime.CharStream;
import org.antlr.runtime.Lexer;
import org.antlr.runtime.NoViableAltException;
import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.RecognizerSharedState;
import org.apache.hadoop.conf.Configuration;

/**
 * Common class of Legacy and SQL Standard Hive Lexer.
 */
public abstract class GenericHiveLexer extends Lexer {

  public static GenericHiveLexer of(String statement, Configuration configuration) {
    GenericHiveLexer lexer;
    if (Quotation.from(configuration) == Quotation.STANDARD) {
      lexer = new HiveLexerStandard(new ANTLRNoCaseStringStream(statement));
    } else {
      lexer = new HiveLexer(new ANTLRNoCaseStringStream(statement));
    }

    lexer.setHiveConf(configuration);
    for (GenericHiveLexer wrappedLexers : lexer.getDelegates()) {
      wrappedLexers.setHiveConf(configuration);
    }

    return lexer;
  }

  private final ArrayList<ParseError> errors;
  private Configuration hiveConf;
  private Quotation quotation;

  public GenericHiveLexer() {
    errors = new ArrayList<>();
  }

  public GenericHiveLexer(CharStream input) {
    super(input);
    errors = new ArrayList<>();
  }

  public GenericHiveLexer(CharStream input, RecognizerSharedState state) {
    super(input, state);
    errors = new ArrayList<>();
  }

  public void setHiveConf(Configuration hiveConf) {
    this.hiveConf = hiveConf;
  }

  public abstract GenericHiveLexer[] getDelegates();

  protected Quotation allowQuotedId() {
    if (quotation == null) {
      quotation = Quotation.from(hiveConf);
    }
    return quotation;
  }

  @Override
  public void displayRecognitionError(String[] tokenNames, RecognitionException e) {
    errors.add(new ParseError(this, e, tokenNames));
  }

  @Override
  public String getErrorMessage(RecognitionException e, String[] tokenNames) {
    String msg;

    if (e instanceof NoViableAltException) {
      //NoViableAltException nvae = (NoViableAltException) e;
      // for development, can add
      // "decision=<<"+nvae.grammarDecisionDescription+">>"
      // and "(decision="+nvae.decisionNumber+") and
      // "state "+nvae.stateNumber
      msg = "character " + getCharErrorDisplay(e.c) + " not supported here";
    } else {
      msg = super.getErrorMessage(e, tokenNames);
    }

    return msg;
  }

  public ArrayList<ParseError> getErrors() {
    return errors;
  }
}
