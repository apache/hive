package org.apache.hadoop.hive.ql.parse;

import org.antlr.runtime.Token;
import org.antlr.runtime.TokenRewriteStream;

import java.util.List;

public class QueryTextExpander extends TokenRewriteStream {
  public static QueryTextExpander with(TokenRewriteStream tokenRewriteStream, UnparseTranslator unparseTranslator) {
    QueryTextExpander queryTextExpander = new QueryTextExpander(tokenRewriteStream);
    unparseTranslator.applyTranslations(queryTextExpander);
    return queryTextExpander;
  }

  private QueryTextExpander(TokenRewriteStream tokenRewriteStream) {
    this.tokens = (List<Token>) tokenRewriteStream.getTokens();
  }
}
