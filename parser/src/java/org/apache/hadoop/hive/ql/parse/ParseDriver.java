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


import org.antlr.runtime.CommonToken;
import org.antlr.runtime.Parser;
import org.antlr.runtime.ParserRuleReturnScope;
import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.Token;
import org.antlr.runtime.TokenRewriteStream;
import org.antlr.runtime.TokenStream;
import org.antlr.runtime.tree.CommonTree;
import org.antlr.runtime.tree.CommonTreeAdaptor;
import org.antlr.runtime.tree.TreeAdaptor;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.function.Function;

/**
 * ParseDriver.
 *
 */
public class ParseDriver {

  private static final Logger LOG = LoggerFactory.getLogger(ParseDriver.class);

  /**
   * Tree adaptor for making antlr return ASTNodes instead of CommonTree nodes
   * so that the graph walking algorithms and the rules framework defined in
   * ql.lib can be used with the AST Nodes.
   */
  public static final TreeAdaptor adaptor = new CommonTreeAdaptor() {
    /**
     * Creates an ASTNode for the given token. The ASTNode is a wrapper around
     * antlr's CommonTree class that implements the Node interface.
     *
     * @param payload
     *          The token.
     * @return Object (which is actually an ASTNode) for the token.
     */
    @Override
    public Object create(Token payload) {
      return new ASTNode(payload);
    }

    @Override
    public Token createToken(int tokenType, String text) {
      if (tokenType == HiveParser.TOK_SETCOLREF) {
        // ParseUtils.processSetColsNode() can change type of TOK_SETCOLREF nodes later
        return new CommonToken(tokenType, text);
      } else {
        return new ImmutableCommonToken(tokenType, text);
      }
    }

    @Override
    public Object dupNode(Object t) {
      return create(((CommonTree)t).token);
    }

    @Override
    public Object dupTree(Object t, Object parent) {
      // Overriden to copy start index / end index, that is needed through optimization,
      // e.g., for masking/filtering
      ASTNode astNode = (ASTNode) t;
      ASTNode astNodeCopy = (ASTNode) super.dupTree(t, parent);
      astNodeCopy.setTokenStartIndex(astNode.getTokenStartIndex());
      astNodeCopy.setTokenStopIndex(astNode.getTokenStopIndex());
      return astNodeCopy;
    }

    @Override
    public Object errorNode(TokenStream input, Token start, Token stop, RecognitionException e) {
      return new ASTErrorNode(input, start, stop, e);
    }
  };

  public ParseResult parse(String command) throws ParseException {
    return parse(command, null);
  }

  /**
   * Parses a command, optionally assigning the parser's token stream to the
   * given context.
   *
   * @param command
   *          command to parse
   *
   * @param configuration
   *          hive configuration
   *
   * @return parsed AST
   */
  public ParseResult parse(String command, Configuration configuration) throws ParseException {
    ASTParser<HiveParser> p = createHiveParser(command, configuration);
    ASTNode t = p.parse(HiveParser::statement);
    t.setUnknownTokenBoundaries();
    return new ParseResult(t, p.tokens, p.parser.gFromClauseParser.tables);
  }

  /*
   * Parse a string as a query hint.
   */
  public ASTNode parseHint(String command) throws ParseException {
    ASTParser<HintParser> p = new ASTParser<HintParser>(command, null, (tokens) -> {
      HintParser hintParser = new HintParser(tokens);
      hintParser.setTreeAdaptor(adaptor);
      return hintParser;
    }) {
      @Override
      Collection<ParseError> parseErrors() {
        return parser.errors;
      }
    };
    return p.parse(HintParser::hint);
  }

  /*
   * parse a String as a Select List. This allows table functions to be passed expression Strings
   * that are translated in
   * the context they define at invocation time. Currently used by NPath to allow users to specify
   * what output they want.
   * NPath allows expressions n 'tpath' a column that represents the matched set of rows. This
   * column doesn't exist in
   * the input schema and hence the Result Expression cannot be analyzed by the regular Hive
   * translation process.
   */
  public ParseResult parseSelect(String command, Configuration configuration) throws ParseException {
    ASTParser<HiveParser> p = createHiveParser(command, configuration);
    ASTNode tree = p.parse(HiveParser::selectClause);
    return new ParseResult(tree, p.tokens, p.parser.gFromClauseParser.tables);
  }

  public ASTNode parseExpression(String command) throws ParseException {
    return createHiveParser(command, null).parse(HiveParser::expression);
  }

  public ASTNode parseTriggerExpression(String command) throws ParseException {
    return createHiveParser(command, null).parse(HiveParser::triggerExpressionStandalone);
  }

  public ASTNode parseTriggerActionExpression(String command) throws ParseException {
    return createHiveParser(command, null).parse(HiveParser::triggerActionExpressionStandalone);
  }

  private interface ParseFunction<T extends Parser> {
    ParserRuleReturnScope apply(T parser) throws RecognitionException;
  }

  private static abstract class ASTParser<T extends Parser> {
    private final GenericHiveLexer lexer;
    protected final TokenRewriteStream tokens;
    protected final T parser;

    ASTParser(String command, Configuration conf, Function<TokenRewriteStream, T> parserFactory) {
      LOG.debug("Parsing command: {}", command);
      this.lexer = GenericHiveLexer.of(command, conf);
      this.tokens = new TokenRewriteStream(lexer);
      this.parser = parserFactory.apply(tokens);
    }

    public ASTNode parse(ParseFunction<T> function) throws ParseException {
      ParserRuleReturnScope r;
      try {
        r = function.apply(parser);
      } catch (RecognitionException e) {
        throw new ParseException(parseErrors());
      }
      if (lexer.getErrors().size() != 0) {
        throw new ParseException(lexer.getErrors());
      } else if (parseErrors().size() != 0) {
        throw new ParseException(parseErrors());
      }
      LOG.debug("Parse Completed");
      return (ASTNode) r.getTree();
    }

    abstract Collection<ParseError> parseErrors();
  }

  private static ASTParser<HiveParser> createHiveParser(String command, Configuration conf) {
    return new ASTParser<HiveParser>(command, conf, (tokens -> {
      HiveParser parser = new HiveParser(tokens);
      parser.setHiveConf(conf);
      parser.setTreeAdaptor(adaptor);
      return parser;
    })) {
      @Override
      Collection<ParseError> parseErrors() {
        return parser.errors;
      }
    };
  }
}
