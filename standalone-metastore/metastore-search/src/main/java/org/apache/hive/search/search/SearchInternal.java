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

  public SearchInternal(SearcherManager manager,
      IndexManager indexManager,
      EmbedModelRegistry registry,
      SearchConfig searchConfig,
      BayesianScoreEstimator.Parameters parameters) throws IOException {
    this.searcherManager = manager;
    this.searcher = manager.acquire();
    this.mapping = indexManager.mapping();
    this.searchConfig = searchConfig;
    this.parameters = parameters;
    this.modelRegistry = Objects.requireNonNull(registry, "Model registry");
  }

  public record RetrieverSpec(Map<String, Object> query, float weight, String name) {
  }

  public record FusionRequest(List<RetrieverSpec> retrievers, int size) {}

  public SearchReqResp.Response search(SearchReqResp.Request request)
      throws SearchException, IOException {
    TopDocs topDocs = executeQuery(request);
    List<Map<String, Object>> hits = new ArrayList<>();
    for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
      hits.add(readHit(scoreDoc, request.fields()));
    }
    return new SearchReqResp.Response(hits, topDocs.totalHits.value());
  }

  private TopDocs executeQuery(SearchReqResp.Request request)
      throws SearchException, IOException {
    int size = request.size() > 0 ? request.size() : searchConfig.getDefaultLimit();
    Query query = compileRootQuery(request.query(), size);
    query = applyScopeFilter(query, request.catalogName(), request.databaseName());
    return searcher.search(query, size);
  }

  private Query compileRootQuery(Map<String, Object> queryNode, int size)
      throws SearchException, IOException {
    if (queryNode.containsKey("hybrid")) {
      return compileHybridQuery(queryNode.get("hybrid"), size);
    }
    return compileQuery(queryNode, size);
  }

  private Query compileHybridQuery(Object hybridBody, int size)
      throws SearchException, IOException {
    HybridSearch.ParsedHybridQuery hybrid = HybridSearch.parse(hybridBody, mapping);
    return compileFusionQuery(HybridSearch.toFusionRequest(hybrid, size, searchConfig), size);
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

  private Query compileQuery(Map<String, Object> queryNode, int knnK)
      throws SearchException, IOException {
    if (queryNode.containsKey("table_keyword")) {
      return compileTableKeywordQuery(queryNode.get("table_keyword").toString());
    }
    if (queryNode.containsKey("semantic")) {
      return compileSemanticQuery(queryNode, knnK);
    }
    if (queryNode.containsKey("hybrid")) {
      throw new SearchException("nested hybrid queries are not supported");
    }
    throw new SearchException("supported queries: table_keyword, match, semantic, hybrid");
  }

  private Query compileTableKeywordQuery(String queryText) throws SearchException, IOException {
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
      throw new SearchException("no lexically searchable table fields are configured");
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
      Map<String, Object> queryNode, int knnK) throws SearchException {
    Object semanticBody = queryNode.get("semantic");
    String field;
    String queryText;
    if (semanticBody instanceof String text) {
      field = requireSemanticField(null);
      queryText = text;
    } else if (semanticBody instanceof Map<?, ?> body) {
      @SuppressWarnings("unchecked")
      Map<String, Object> semanticMap = (Map<String, Object>) body;
      if (semanticMap.containsKey("field") && semanticMap.containsKey("query")) {
        field = semanticMap.get("field").toString();
        queryText = semanticMap.get("query").toString();
      } else if (semanticMap.size() == 1) {
        Map.Entry<String, Object> entry = semanticMap.entrySet().iterator().next();
        field = entry.getKey();
        queryText = entry.getValue().toString();
      } else {
        throw new SearchException("semantic query must be {field: text} or {field:, query:}");
      }
    } else {
      throw new SearchException("semantic query must be a string or object");
    }

    FieldSchema schema = mapping.fieldSchema(field);
    if (!(schema instanceof FieldSchema.TextFieldSchema text) || !text.search().semantic()) {
      throw new SearchException("field '" + field + "' is not semantically searchable");
    }
    float[] embedding;
    try {
      embedding = modelRegistry.get(text.search().semanticModel())
          .encode(EmbedModel.TaskType.QUERY, queryText);
    } catch (IndexException e) {
      throw new SearchException(
          "Failed to encode semantic query for field '" + field + "'", e);
    }
    if (embedding == null) {
      throw new SearchException(
          "embedding model '" + text.search().semanticModel() + "' returned null vector");
    }
    return new KnnFloatVectorQuery(field, embedding, knnK);
  }

  private String requireSemanticField(String field) throws SearchException {
    if (StringUtils.isNotEmpty(field)) {
      validateSemanticField(field);
      return field;
    }
    List<String> semanticFields = mapping.fields().entrySet().stream()
        .filter(entry -> entry.getValue() instanceof FieldSchema.TextFieldSchema text
            && text.search().semantic())
        .map(java.util.Map.Entry::getKey)
        .toList();
    if (semanticFields.size() == 1) {
      return semanticFields.getFirst();
    }
    throw new SearchException(
        "semantic query requires field when index has "
            + semanticFields.size()
            + " semantic field(s): "
            + semanticFields);
  }

  private void validateSemanticField(String field) throws SearchException {
    FieldSchema schema = mapping.fieldSchema(field);
    if (!(schema instanceof FieldSchema.TextFieldSchema text) || !text.search().semantic()) {
      throw new SearchException("field '" + field + "' is not semantically searchable");
    }
  }

  private Map<String, Object> readHit(ScoreDoc scoreDoc, List<String> fields)
      throws IOException {
    Document stored = searcher.storedFields().document(scoreDoc.doc);
    Map<String, Object> hit = new LinkedHashMap<>();
    hit.put("_score", scoreDoc.score);
    List<String> requested = fields.isEmpty() ? mapping.fields().keySet().stream().toList() : fields;
    for (String field : requested) {
      IndexableField[] values = stored.getFields(field);
      if (values != null && values.length > 0) {
        hit.put(field, values[0].stringValue());
      }
    }
    IndexableField[] idValues = stored.getFields("_id");
    if (idValues != null && idValues.length > 0) {
      hit.put("_id", idValues[0].stringValue());
    }
    return hit;
  }

  @Override
  public void close() throws IOException {
    searcherManager.release(searcher);
  }
}
