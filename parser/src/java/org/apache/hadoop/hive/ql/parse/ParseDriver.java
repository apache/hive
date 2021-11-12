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


import java.util.List;
import org.antlr.runtime.CommonToken;
import org.antlr.runtime.ParserRuleReturnScope;
import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.Token;
import org.antlr.runtime.TokenRewriteStream;
import org.antlr.runtime.TokenStream;
import org.antlr.runtime.tree.CommonTree;
import org.antlr.runtime.tree.CommonTreeAdaptor;
import org.antlr.runtime.tree.TreeAdaptor;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  public ParseResult parse(String command, Configuration configuration)
      throws ParseException {
    LOG.debug("Parsing command: {}", command);

    GenericHiveLexer lexer = GenericHiveLexer.of(command, configuration);
    TokenRewriteStream tokens = new TokenRewriteStream(lexer);
    HiveParser parser = new HiveParser(tokens);
    parser.setTreeAdaptor(adaptor);
    parser.setHiveConf(configuration);
    ParserRuleReturnScope r;
    try {
      r = parser.statement();
    } catch (RecognitionException e) {
      throw new ParseException(parser.errors);
    }

    if (lexer.getErrors().size() == 0 && parser.errors.size() == 0) {
      LOG.debug("Parse Completed");
    } else if (lexer.getErrors().size() != 0) {
      throw new ParseException(lexer.getErrors());
    } else {
      throw new ParseException(parser.errors);
    }

    ASTNode tree = (ASTNode) r.getTree();
    tree.setUnknownTokenBoundaries();
    return new ParseResult(tree, tokens, parser.gFromClauseParser.tables);
  }

  /*
   * Parse a string as a query hint.
   */
  public ASTNode parseHint(String command) throws ParseException {
    LOG.debug("Parsing hint: {}", command);

    GenericHiveLexer lexer = GenericHiveLexer.of(command, null);
    TokenRewriteStream tokens = new TokenRewriteStream(lexer);
    HintParser parser = new HintParser(tokens);
    parser.setTreeAdaptor(adaptor);
    HintParser.hint_return r = null;
    try {
      r = parser.hint();
    } catch (RecognitionException e) {
      throw new ParseException(parser.errors);
    }

    if (lexer.getErrors().size() == 0 && parser.errors.size() == 0) {
      LOG.debug("Parse Completed");
    } else if (lexer.getErrors().size() != 0) {
      throw new ParseException(lexer.getErrors());
    } else {
      throw new ParseException(parser.errors);
    }

    return (ASTNode) r.getTree();
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
    LOG.debug("Parsing command: {}", command);

    GenericHiveLexer lexer = GenericHiveLexer.of(command, configuration);
    TokenRewriteStream tokens = new TokenRewriteStream(lexer);
//    if (ctx != null) {
//      ctx.setTokenRewriteStream(tokens);
//    }
    HiveParser parser = new HiveParser(tokens);
    parser.setTreeAdaptor(adaptor);
    parser.setHiveConf(configuration);
    ParserRuleReturnScope r;
    try {
      r = parser.selectClause();
    } catch (RecognitionException e) {
      throw new ParseException(parser.errors);
    }

    if (lexer.getErrors().size() == 0 && parser.errors.size() == 0) {
      LOG.debug("Parse Completed");
    } else if (lexer.getErrors().size() != 0) {
      throw new ParseException(lexer.getErrors());
    } else {
      throw new ParseException(parser.errors);
    }

    return new ParseResult((ASTNode) r.getTree(), tokens, parser.gFromClauseParser.tables);
  }
  public ASTNode parseExpression(String command) throws ParseException {
    LOG.debug("Parsing expression: {}", command);

    GenericHiveLexer lexer = GenericHiveLexer.of(command, null);
    TokenRewriteStream tokens = new TokenRewriteStream(lexer);
    HiveParser parser = new HiveParser(tokens);
    parser.setTreeAdaptor(adaptor);
    ParserRuleReturnScope r;
    try {
      r = parser.expression();
    } catch (RecognitionException e) {
      throw new ParseException(parser.errors);
    }

    if (lexer.getErrors().size() == 0 && parser.errors.size() == 0) {
      LOG.debug("Parse Completed");
    } else if (lexer.getErrors().size() != 0) {
      throw new ParseException(lexer.getErrors());
    } else {
      throw new ParseException(parser.errors);
    }

    return (ASTNode) r.getTree();
  }

  public ASTNode parseTriggerExpression(String command) throws ParseException {
    GenericHiveLexer lexer = GenericHiveLexer.of(command, null);
    TokenRewriteStream tokens = new TokenRewriteStream(lexer);
    HiveParser parser = new HiveParser(tokens);
    parser.setTreeAdaptor(adaptor);
    ParserRuleReturnScope r;
    try {
      r = parser.triggerExpressionStandalone();
    } catch (RecognitionException e) {
      throw new ParseException(parser.errors);
    }
    if (lexer.getErrors().size() != 0) {
      throw new ParseException(lexer.getErrors());
    } else if (parser.errors.size() != 0) {
      throw new ParseException(parser.errors);
    }

    return (ASTNode) r.getTree();
  }

  public ASTNode parseTriggerActionExpression(String command) throws ParseException {
    GenericHiveLexer lexer = GenericHiveLexer.of(command, null);
    TokenRewriteStream tokens = new TokenRewriteStream(lexer);
    HiveParser parser = new HiveParser(tokens);
    parser.setTreeAdaptor(adaptor);
    ParserRuleReturnScope r;
    try {
      r = parser.triggerActionExpressionStandalone();
    } catch (RecognitionException e) {
      throw new ParseException(parser.errors);
    }
    if (lexer.getErrors().size() != 0) {
      throw new ParseException(lexer.getErrors());
    } else if (parser.errors.size() != 0) {
      throw new ParseException(parser.errors);
    }

    return (ASTNode) r.getTree();
  }
}
