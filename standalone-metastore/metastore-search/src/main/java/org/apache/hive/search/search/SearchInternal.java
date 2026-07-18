/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hive.search.search;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hive.search.config.SearchConfig;
import org.apache.hive.search.exception.IndexException;
import org.apache.hive.search.exception.SearchException;
import org.apache.hive.search.mapping.FieldSchema;
import org.apache.hive.search.mapping.IndexMapping;
import org.apache.hive.search.mapping.TableDocument;
import org.apache.hive.search.metastore.MetastoreTableMapper;
import org.apache.hive.search.index.IndexManager;
import org.apache.hive.search.inference.EmbedModel;
import org.apache.hive.search.inference.EmbedModelRegistry;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BayesianScoreEstimator;
import org.apache.lucene.search.BayesianScoreQuery;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.LogOddsFusionQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.TopDocs;

public final class SearchInternal implements AutoCloseable {
  private final EmbedModelRegistry modelRegistry;
  private final IndexSearcher searcher;
  private final SearcherManager searcherManager;
  private final IndexMapping mapping;
  private final SearchConfig searchConfig;
  private final BayesianScoreEstimator.Parameters parameters;
  private final long indexedNid;

  public SearchInternal(SearcherManager manager,
      IndexManager indexManager,
      EmbedModelRegistry registry,
      SearchConfig searchConfig,
      BayesianScoreEstimator.Parameters parameters) throws IOException {
    this.searcherManager = manager;
    this.indexedNid = indexManager.getIndexedNid();
    this.searcher = manager.acquire();
    this.mapping = indexManager.mapping();
    this.searchConfig = searchConfig;
    this.parameters = parameters;
    this.modelRegistry = Objects.requireNonNull(registry, "Model registry");
  }

  public record RetrieverSpec(SearchArgs query, float weight, String name) {
  }

  public record FusionRequest(List<RetrieverSpec> retrievers, int size) {}

  public TableSearchResult search(SearchQuery request)
      throws SearchException, IOException {
    int size = request.limit() > 0 ? request.limit() : searchConfig.getDefaultLimit();
    Query query = compileRootQuery(request, size);
    query = applyScopeFilter(query, request.catalogName(), request.databaseName());
    TopDocs topDocs = searcher.search(query, size);
    List<TableSearchHit> hits = new ArrayList<>();
    for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
      hits.add(readHit(scoreDoc, request.returnFields()));
    }
    return new TableSearchResult(hits, topDocs.totalHits.value(), indexedNid);
  }

  private Query compileRootQuery(SearchQuery searchQuery, int size)
      throws SearchException, IOException {
    return switch (searchQuery.args()) {
      case SearchArgs.Match match -> compileMatchQuery(match.queryText());
      case SearchArgs.Semantic semantic ->
          compileSemanticQuery(SemanticSearch.resolve(semantic, mapping), size);
      case SearchArgs.Hybrid hybrid -> {
        HybridSearch.ResolvedHybridQuery resolved = HybridSearch.resolve(hybrid, mapping);
        yield compileFusionQuery(
            HybridSearch.toFusionRequest(resolved, size, searchConfig), size);
      }
    };
  }

  private Query compileFusionQuery(FusionRequest fusion, int size)
      throws SearchException, IOException {
    int i = 0;
    float[] weights = new float[fusion.retrievers.size()];
    List<Query> queries = new ArrayList<>();
    for (RetrieverSpec retriever : fusion.retrievers()) {
      Query internalQuery = compileQuery(retriever.query(), size);
      if (internalQuery instanceof KnnFloatVectorQuery) {
        queries.add(internalQuery);
      } else {
        queries.add(new BayesianScoreQuery(internalQuery, parameters.alpha(),
            parameters.beta(), parameters.baseRate()));
      }
      weights[i++] = retriever.weight();
    }
    return new LogOddsFusionQuery(queries, searchConfig.getFusionPrior(), weights);
  }

  private Query applyScopeFilter(Query query, String catalogName, String databaseName) {
    Query filter = compileScopeFilter(catalogName, databaseName);
    if (filter == null) {
      return query;
    }
    return new BooleanQuery.Builder()
        .add(query, BooleanClause.Occur.MUST)
        .add(filter, BooleanClause.Occur.FILTER)
        .build();
  }

  private Query compileScopeFilter(String catalogName, String databaseName) {
    boolean hasCatalog = StringUtils.isNotEmpty(catalogName);
    boolean hasDatabase = StringUtils.isNotEmpty(databaseName);
    if (!hasCatalog && !hasDatabase) {
      return null;
    }
    if (hasCatalog && hasDatabase) {
      return new PrefixQuery(new Term("_id" + TableDocument.FILTER_SUFFIX,
          catalogName + "." + databaseName + "."));
    }
    if (hasCatalog) {
      return new PrefixQuery(new Term("_id" + TableDocument.FILTER_SUFFIX, catalogName + "."));
    }
    return new TermQuery(new Term(
        MetastoreTableMapper.FIELD_DB + TableDocument.FILTER_SUFFIX, databaseName));
  }

  private Query compileQuery(SearchArgs args, int knnK)
      throws SearchException, IOException {
    return switch (args) {
      case SearchArgs.Match match -> compileMatchQuery(match.queryText());
      case SearchArgs.Semantic semantic ->
          compileSemanticQuery(SemanticSearch.resolve(semantic, mapping), knnK);
      case SearchArgs.Hybrid hybrid ->
          throw new SearchException("Nested hybrid queries are not supported");
    };
  }

  private Query compileMatchQuery(String queryText) throws SearchException, IOException {
    BooleanQuery.Builder builder = new BooleanQuery.Builder();
    boolean added = false;
    for (MetastoreTableMapper.KeywordSearchField keywordField : MetastoreTableMapper.KEYWORD_SEARCH_FIELDS) {
      String field = keywordField.field();
      float boost = keywordField.boost();
      FieldSchema schema = mapping.fieldSchema(field);
      if (!(schema instanceof FieldSchema.TextFieldSchema text)) {
        continue;
      }
      if (text.filter()) {
        builder.add(
            boostKeywordQuery(compileFilterKeywordQuery(field, queryText), boost),
            BooleanClause.Occur.SHOULD);
        if (text.search().lexical()) {
          builder.add(
              boostKeywordQuery(compileMatchQuery(field, queryText), boost),
              BooleanClause.Occur.SHOULD);
        }
        added = true;
        continue;
      }
      if (!text.search().lexical()) {
        continue;
      }
      builder.add(
          boostKeywordQuery(compileMatchQuery(field, queryText), boost),
          BooleanClause.Occur.SHOULD);
      added = true;
    }
    if (!added) {
      throw new SearchException("No lexically searchable table fields are configured");
    }
    return builder.build();
  }

  private static Query boostKeywordQuery(Query query, float boost) {
    return new BoostQuery(new ConstantScoreQuery(query), boost);
  }

  private Query compileFilterKeywordQuery(String field, String queryText) {
    String normalized = queryText.trim().toLowerCase(Locale.ROOT);
    if (normalized.isEmpty()) {
      return new BooleanQuery.Builder().build();
    }
    return new TermQuery(new Term(field + TableDocument.FILTER_SUFFIX, normalized));
  }

  private Query compileMatchQuery(String field, String queryText)
      throws SearchException, IOException {

    FieldSchema schema = mapping.fieldSchema(field);
    if (!(schema instanceof FieldSchema.TextFieldSchema text) || !text.search().lexical()) {
      throw new SearchException("field '" + field + "' is not lexically searchable");
    }

    Analyzer analyzer = mapping.analyzer();
    BooleanQuery.Builder builder = new BooleanQuery.Builder();
    try (TokenStream stream = analyzer.tokenStream(field, queryText)) {
      CharTermAttribute termAttr = stream.addAttribute(CharTermAttribute.class);
      stream.reset();
      while (stream.incrementToken()) {
        builder.add(new TermQuery(new Term(field, termAttr.toString())), BooleanClause.Occur.SHOULD);
      }
      stream.end();
    }
    return builder.build();
  }

  private org.apache.lucene.search.Query compileSemanticQuery(
      SemanticSearch.ResolvedSemanticQuery semantic, int knnK) throws SearchException {
    FieldSchema schema = mapping.fieldSchema(semantic.field());
    if (!(schema instanceof FieldSchema.TextFieldSchema text) || !text.search().semantic()) {
      throw new SearchException("field '" + semantic.field() + "' is not semantically searchable");
    }
    float[] embedding;
    try {
      embedding = modelRegistry.get(text.search().semanticModel())
          .embed(EmbedModel.TaskType.QUERY, semantic.queryText());
    } catch (IndexException e) {
      throw new SearchException(
          "Failed to encode semantic query for field '" + semantic.field() + "'", e);
    }
    if (embedding == null) {
      throw new SearchException(
          "embedding model '" + text.search().semanticModel() + "' returned null vector");
    }
    return new KnnFloatVectorQuery(semantic.field(), embedding, knnK);
  }

  private TableSearchHit readHit(ScoreDoc scoreDoc, List<String> fields)
      throws IOException {
    Document stored = searcher.storedFields().document(scoreDoc.doc);
    Map<String, String> fieldHits = new LinkedHashMap<>();
    List<String> requested = fields.isEmpty() ?
        mapping.fields().keySet().stream().toList() : fields;
    for (String field : requested) {
      IndexableField[] values = stored.getFields(field);
      if (values != null && values.length > 0) {
        fieldHits.put(field, values[0].stringValue());
      }
    }
    TableName tableName = null;
    IndexableField[] idValues = stored.getFields("_id");
    if (idValues != null && idValues.length > 0) {
      tableName = TableName.fromString(idValues[0].stringValue(), "", "default");
    }
    return new TableSearchHit(tableName, scoreDoc.score, fieldHits);
  }

  @Override
  public void close() throws IOException {
    searcherManager.release(searcher);
  }
}
