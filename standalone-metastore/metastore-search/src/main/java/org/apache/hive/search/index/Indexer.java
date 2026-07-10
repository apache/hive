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

package org.apache.hive.search.index;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.hadoop.hive.common.DatabaseName;
import org.apache.hadoop.hive.metastore.Batchable;
import org.apache.hive.search.config.IndexConfig;
import org.apache.hive.search.exception.IndexException;
import org.apache.hive.search.inference.EmbedModel;
import org.apache.hive.search.inference.EmbedModelRegistry;
import org.apache.hive.search.inference.EmbeddingCache;
import org.apache.hive.search.mapping.FieldSchema;
import org.apache.hive.search.mapping.TableDocument;
import org.apache.hive.search.mapping.field.Field;
import org.apache.hive.search.mapping.field.TextField;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.KeepOnlyLastCommitDeletionPolicy;
import org.apache.lucene.index.SnapshotDeletionPolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Indexer implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(Indexer.class);
  private static final int EMBED_BATCH_SIZE = 10000;

  private final IndexManager indexManager;
  private final EmbedModelRegistry modelRegistry;
  private final EmbeddingCache embeddingCache;
  private final int commitFlushThreshold;
  private SnapshotDeletionPolicy snapshotter;
  private FlushTrackingWriter writer;

  public Indexer(IndexManager index, EmbedModelRegistry registry) {
    this.indexManager = index;
    this.modelRegistry = registry;
    this.embeddingCache = registry.embeddingCache();
    this.commitFlushThreshold =
        new IndexConfig(index.mapping().configuration()).getCommitFlushes();
  }

  int flushesSinceCommit() {
    return writer.flushesSinceCommit();
  }

  public void initialize() throws IOException {
    IndexWriterConfig config =
        new IndexWriterConfig(indexManager.mapping().analyzer())
            .setCommitOnClose(false)
            .setRAMBufferSizeMB(indexManager.mapping().config().getWriteBufferSizeMb());
    SnapshotDeletionPolicy snapshotDeletionPolicy =
        new SnapshotDeletionPolicy(new KeepOnlyLastCommitDeletionPolicy());
    config.setIndexDeletionPolicy(snapshotDeletionPolicy);
    this.snapshotter = snapshotDeletionPolicy;
    this.writer = new FlushTrackingWriter(indexManager.directory(), config);
  }

  public void syncBackup() throws IOException {
    if (!indexManager.hasBackup()) {
      return;
    }
    IndexCommit snapshot = snapshotter.snapshot();
    try {
      indexManager.syncBackup();
    } finally {
      snapshotter.release(snapshot);
    }
  }

  /** Writes already-embedded documents to Lucene. */
  private void writeDocuments(List<TableDocument> docs) throws IOException, IndexException {
    List<Document> luceneDocs = new ArrayList<>();
    List<String> ids = new ArrayList<>();
    for (TableDocument doc : docs) {
      ids.add(doc.idField().value());
      luceneDocs.addAll(doc.toDocuments());
    }
    delete(ids.toArray(new String[0]));
    writer.addDocuments(luceneDocs);
  }

  public List<TableDocument> embedDocuments(List<TableDocument> tableDocs)
      throws IndexException {
    long start = System.currentTimeMillis();
    List<TableDocument> result = new ArrayList<>(tableDocs.size());
    Map<String, ListMultimap<TextField, TableDocument>> modelPerTxt = new HashMap<>();
    for (TableDocument doc : tableDocs) {
      TableDocument newDoc = new TableDocument(doc.idField(), List.of(), indexManager.mapping());
      result.add(newDoc);
      for (Field field : doc.fields()) {
        if (field instanceof org.apache.hive.search.mapping.field.IdField) {
          continue;
        }
        if (field instanceof TextField text) {
          FieldSchema schema = indexManager.mapping().fieldSchema(text.name());
          if (schema instanceof FieldSchema.TextFieldSchema textSchema
              && textSchema.search().semantic()) {
            if (text.embedding() != null) {
              newDoc.appendField(field);
            } else {
              String modelRef = textSchema.search().semanticModel();
              ListMultimap<TextField, TableDocument> multimapText =
                  modelPerTxt.computeIfAbsent(modelRef, s -> ArrayListMultimap.create());
              multimapText.put(text, newDoc);
            }
          } else {
            newDoc.appendField(field);
          }
        } else {
          newDoc.appendField(field);
        }
      }
    }
    int totalFields = modelPerTxt.values().stream().mapToInt(ListMultimap::size).sum();
    long cacheHitsBefore = embeddingCache.hits();
    long cacheMissesBefore = embeddingCache.misses();
    for (Map.Entry<String, ListMultimap<TextField, TableDocument>> entry : modelPerTxt.entrySet()) {
      embedInBatch(entry.getKey(), modelRegistry.get(entry.getKey()), entry.getValue());
    }
    if (totalFields > 0) {
      long cacheHits = embeddingCache.hits() - cacheHitsBefore;
      long cacheMisses = embeddingCache.misses() - cacheMissesBefore;
      LOG.info("Embedded {} semantic field(s) across {} document(s) in {}ms"
              + " (embedding cache hits={}, misses={})",
          totalFields, tableDocs.size(), System.currentTimeMillis() - start, cacheHits, cacheMisses);
    }
    return result;
  }

  private void embedInBatch(String modelRef, EmbedModel embedModel,
      ListMultimap<TextField, TableDocument> textDocs) throws IndexException {
    int uniqueTexts = textDocs.keySet().stream()
        .map(TextField::value)
        .collect(java.util.stream.Collectors.toSet())
        .size();
    long start = System.currentTimeMillis();
    try {
      Batchable.runBatched(EMBED_BATCH_SIZE, new ArrayList<>(textDocs.keySet()),
          new Batchable<TextField, Void>() {
        @Override
        public List<Void> run(List<TextField> batchFields) throws IndexException {
          long batchStart = System.currentTimeMillis();
          ListMultimap<String, TextField> valueToTxt = ArrayListMultimap.create();
          batchFields.forEach(f -> valueToTxt.put(f.value(), f));
          List<String> missTexts = new ArrayList<>();
          for (String text : valueToTxt.keySet()) {
            Optional<float[]> cached =
                embeddingCache.get(modelRef, EmbedModel.TaskType.DOCUMENT, text);
            if (cached.isPresent()) {
              applyEmbedding(valueToTxt, textDocs, text, cached.get());
            } else {
              missTexts.add(text);
            }
          }
          if (!missTexts.isEmpty()) {
            String[] texts = missTexts.toArray(new String[0]);
            float[][] embeddings = embedModel.embedBatch(EmbedModel.TaskType.DOCUMENT, texts);
            for (int i = 0; i < texts.length; i++) {
              String text = texts[i];
              float[] embedding = embeddings[i];
              embeddingCache.put(modelRef, EmbedModel.TaskType.DOCUMENT, text, embedding);
              applyEmbedding(valueToTxt, textDocs, text, embedding);
            }
          }
          LOG.debug("Model '{}' embedded batch of {} unique text(s), {} cache miss(es) in {}ms",
              modelRef, valueToTxt.keySet().size(), missTexts.size(),
              System.currentTimeMillis() - batchStart);
          return List.of();
        }
      });
    } catch (Exception e) {
      throw IndexException.wrap("Error while embedding the documents with model '" + modelRef + "'",
          e);
    }
    LOG.info("Model '{}' embedded {} field(s) from {} unique text(s) in {}ms",
        modelRef, textDocs.size(), uniqueTexts, System.currentTimeMillis() - start);
  }

  private void applyEmbedding(ListMultimap<String, TextField> valueToTxt,
      ListMultimap<TextField, TableDocument> textDocs, String text, float[] embedding) {
    for (TextField tf : valueToTxt.get(text)) {
      for (TableDocument document : textDocs.get(tf)) {
        document.appendField(tf.withEmbedding(embedding));
      }
    }
  }

  public void addDocuments(List<TableDocument> docs) throws IOException, IndexException {
    writeDocuments(embedDocuments(docs));
  }

  public IndexWriter writer() {
    return writer;
  }

  public EmbeddingCache embeddingCache() {
    return embeddingCache;
  }

  /**
   * Commits the index when forced or after enough Lucene auto-flushes.
   * Lucene continues to flush segments automatically based on RAM buffer settings.
   */
  public boolean flush(long lastEventId, boolean force)
      throws IOException {
    if (!force && (!hasPendingChanges() || !shouldCommit())) {
      return false;
    }
    String model = indexManager.mapping().inference().modelName();
    Map<String, String> metadata = Map.of(
        "nid", lastEventId + "",
        "model", model,
        "commit_time", String.valueOf(System.currentTimeMillis())
    );
    writer.setLiveCommitData(metadata.entrySet());
    long seqnum = writer.commit();
    if (seqnum < 0) {
      return false;
    }
    writer.resetFlushTracking();
    return true;
  }

  private boolean shouldCommit() {
    return commitFlushThreshold <= 0 || writer.flushesSinceCommit() >= commitFlushThreshold;
  }

  private boolean hasPendingChanges() {
    return writer.hasUncommittedChanges()
        || writer.numRamDocs() > 0
        || writer.hasDeletions();
  }

  public int delete(String... docIds) throws IOException {
    if (docIds == null || docIds.length == 0) {
      return 0;
    }
    int before = writer.getDocStats().numDocs;
    Term[] terms = Arrays.stream(docIds)
        .map(id -> new Term("_id" + TableDocument.FILTER_SUFFIX, id))
        .toArray(Term[]::new);
    writer.deleteDocuments(terms);
    int after = writer.getDocStats().numDocs;
    return before - after;
  }

  public int deleteDatabases(DatabaseName... databases)
      throws IOException {
    if (databases == null || databases.length == 0) {
      return 0;
    }
    int before = writer.getDocStats().numDocs;
    BooleanQuery.Builder builder = new BooleanQuery.Builder();
    for (DatabaseName database : databases) {
      Query query = new PrefixQuery(new Term("_id" + TableDocument.FILTER_SUFFIX,
          database.getCat() + "." + database.getDb() + "."));
      builder.add(query, BooleanClause.Occur.SHOULD);
    }
    writer.deleteDocuments(builder.build());
    int after = writer.getDocStats().numDocs;
    return before - after;
  }

  @Override
  public void close() throws IOException {
    writer.close();
  }
}
