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
import org.apache.hive.search.exception.InferenceException;
import org.apache.hive.search.exception.SearchException;
import org.apache.hive.search.mapping.FieldSchema;
import org.apache.hive.search.mapping.IndexMapping;
import org.apache.hive.search.mapping.TableDocument;
import org.apache.hive.search.metastore.MetastoreTableMapper;
import org.apache.hive.search.index.IndexManager;
import org.apache.hive.search.inference.Embedder;
import org.apache.hive.search.inference.EmbedderRegistry;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.simple.SimpleQueryParser;
import org.apache.lucene.search.BayesianScoreEstimator;
import org.apache.lucene.search.BayesianScoreQuery;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.LogOddsFusionQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.TopDocs;

public final class Searcher implements AutoCloseable {
  private final EmbedderRegistry embedderRegistry;
  private final IndexSearcher searcher;
  private final SearcherManager searcherManager;
  private final IndexMapping mapping;
  private final SearchConfig searchConfig;
  private final BayesianScoreEstimator.Parameters parameters;
  private final long committedEventId;
  private final long processedEventId;

  public Searcher(SearcherManager manager,
      IndexManager indexManager,
      EmbedderRegistry registry,
      SearchConfig searchConfig,
      BayesianScoreEstimator.Parameters parameters) throws IOException {
    this.searcherManager = manager;
    this.committedEventId = indexManager.getCommittedEventId();
    this.processedEventId = indexManager.getProcessedEventId();
    this.searcher = manager.acquire();
    this.mapping = indexManager.mapping();
    this.searchConfig = searchConfig;
    this.parameters = parameters;
    this.embedderRegistry = Objects.requireNonNull(registry, "Embedder registry");
  }

  public TableSearchResult search(SearchQuery request)
      throws SearchException, IOException {
    if (StringUtils.isBlank(request.body().queryText())) {
      throw new SearchException("query text is required");
    }
    int size = request.limit() > 0 ? request.limit() : searchConfig.getDefaultLimit();
    int semanticK = searchConfig.semanticK(size);
    Query query =
        switch (request.body()) {
          case MatchQuery match -> compileMatchQuery(match.resolve(mapping));
          case SemanticQuery semantic -> compileSemanticQuery(semantic.resolve(mapping), semanticK);
          case HybridQuery hybrid -> compileFusionQuery(hybrid.resolve(mapping), semanticK);
        };
    query = applyScopeFilter(query, request.catalogName(), request.databaseName());
    TopDocs topDocs = searcher.search(query, size);
    List<TableSearchHit> hits = new ArrayList<>();
    for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
      hits.add(readHit(scoreDoc, request.returnFields()));
    }
    return new TableSearchResult(hits, topDocs.totalHits.value(), committedEventId, processedEventId);
  }

  private Query compileFusionQuery(HybridQuery query, int semanticK)
      throws SearchException, IOException {
    Query matchQuery = new BayesianScoreQuery(
        compileMatchQuery(query.toMatchQuery().resolve(mapping)),
        parameters.alpha(), parameters.beta(), parameters.baseRate());
    Query semanticQuery = compileSemanticQuery(query.toSemanticQuery().resolve(mapping), semanticK);
    float semanticWeight = query.semanticWeight(searchConfig);
    float[] weights = {1.0f - semanticWeight, semanticWeight};
    return new LogOddsFusionQuery(List.of(matchQuery, semanticQuery),
        searchConfig.getFusionPrior(), weights);
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

  private Query compileMatchQuery(MatchQuery match)
      throws SearchException {
    if (StringUtils.isNotEmpty(match.field())) {
      return booleanShould(lexicalClausesForField(match.field(), match.queryText(), 1.0f),
          "field '" + match.field() + "' is not lexically searchable");
    }
    BooleanQuery.Builder builder = new BooleanQuery.Builder();
    boolean added = false;
    for (MetastoreTableMapper.KeywordSearchField keywordField : MetastoreTableMapper.KEYWORD_SEARCH_FIELDS) {
      for (Query clause : lexicalClausesForField(
          keywordField.field(), match.queryText(), keywordField.boost())) {
        builder.add(clause, BooleanClause.Occur.SHOULD);
        added = true;
      }
    }
    if (!added) {
      throw new SearchException("No lexically searchable table fields are configured");
    }
    return builder.build();
  }

  private Query booleanShould(List<Query> clauses, String ifEmptyMessage) throws SearchException {
    if (clauses.isEmpty()) {
      throw new SearchException(ifEmptyMessage);
    }
    if (clauses.size() == 1) {
      return clauses.getFirst();
    }
    BooleanQuery.Builder builder = new BooleanQuery.Builder();
    for (Query clause : clauses) {
      builder.add(clause, BooleanClause.Occur.SHOULD);
    }
    return builder.build();
  }

  private List<Query> lexicalClausesForField(String field, String queryText, float boost)
      throws SearchException {
    FieldSchema schema = mapping.fieldSchema(field);
    if (!(schema instanceof FieldSchema.TextFieldSchema text)) {
      return List.of();
    }
    List<Query> clauses = new ArrayList<>();
    if (text.filter()) {
      clauses.add(maybeBoost(compileFilterKeywordQuery(field, queryText), boost));
      if (text.search().lexical()) {
        clauses.add(maybeBoost(compileFieldMatchQuery(field, queryText), boost));
      }
      return clauses;
    }
    if (text.search().lexical()) {
      clauses.add(maybeBoost(compileFieldMatchQuery(field, queryText), boost));
    }
    return clauses;
  }

  private Query maybeBoost(Query query, float boost) {
    return boost == 1.0f ? query : boostKeywordQuery(query, boost);
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

  private Query compileFieldMatchQuery(String field, String queryText)
      throws SearchException {
    FieldSchema schema = mapping.fieldSchema(field);
    if (!(schema instanceof FieldSchema.TextFieldSchema text) || !text.search().lexical()) {
      throw new SearchException("field '" + field + "' is not lexically searchable");
    }
    return new SimpleQueryParser(mapping.analyzer(), field).parse(queryText);
  }

  private Query compileSemanticQuery(SemanticQuery semantic, int knnK) throws SearchException {
    List<String> fields = mapping.resolveSemanticSearchFields(null);
    FieldSchema schema = mapping.fieldSchema(fields.getFirst());
    if (!(schema instanceof FieldSchema.TextFieldSchema text) || !text.search().semantic()) {
      throw new SearchException("index has no semantically searchable fields configured");
    }
    float[] embedding;
    try {
      embedding = embedderRegistry.get(text.search().semanticModel())
          .embed(Embedder.TaskType.QUERY, semantic.queryText());
    } catch (InferenceException e) {
      throw new SearchException("Failed to encode semantic query", e);
    }
    if (embedding == null) {
      throw new SearchException(
          "embedding model '" + text.search().semanticModel() + "' returned null vector");
    }
    if (fields.size() == 1) {
      return new KnnFloatVectorQuery(fields.getFirst(), embedding, knnK);
    }
    List<Query> disjuncts = new ArrayList<>(fields.size());
    for (String field : fields) {
      disjuncts.add(new KnnFloatVectorQuery(field, embedding, knnK));
    }
    return new DisjunctionMaxQuery(disjuncts, 0.0f);
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
