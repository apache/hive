/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.mr.hive.variant;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.And;
import org.apache.iceberg.expressions.Binder;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.InclusiveMetricsEvaluator;
import org.apache.iceberg.expressions.Not;
import org.apache.iceberg.expressions.UnboundExtract;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

/**
 * Drops data files whose variant bounds show that no row can match the {@code extract(...)} predicates of a
 * scan filter.
 *
 * <p>A bridge until apache/iceberg#18518 is in Hive's Iceberg version. Before it, Iceberg planning cannot
 * describe an {@code extract(...)} term and reads the little-endian variant bounds of a manifest in a ByteBuffer's
 * default big-endian order, so Hive strips these predicates from the planning filter. Once #18518 is available,
 * keep {@code extract(...)} in the planning filter and delete this class and its hook in {@code IcebergInputFormat}.
 *
 * <p>Variant string bounds written by Iceberg 1.10 and 1.11 can be inverted (apache/iceberg#17397); they are
 * trusted here, as Iceberg's own planning does.
 */
public final class VariantFilePruner {

  private final InclusiveMetricsEvaluator evaluator;
  private final Set<Integer> variantFieldIds;
  private final List<String> variantColumns;

  private VariantFilePruner(Schema schema, Expression extractFilter, boolean caseSensitive) {
    this.evaluator = new InclusiveMetricsEvaluator(schema, extractFilter, caseSensitive);
    this.variantFieldIds = Binder.boundReferences(schema.asStruct(), extractFilter, caseSensitive);
    this.variantColumns = variantFieldIds.stream().map(schema::findColumnName).toList();
  }

  /**
   * Returns a pruner for the top-level conjuncts of the filter that are an {@code extract(...)} predicate, or the NOT
   * of one, on a variant column of the scan's schema; null if there are none. A time-travel scan can predate the
   * variant column, and {@code variant_get} also accepts struct columns.
   *
   * <p>Hive's search argument writes {@code >}, {@code >=}, {@code <>} and {@code IS NOT NULL} as the NOT of a
   * predicate, which the evaluator negates exactly. All other conjuncts, OR, and NOT over anything else are ignored,
   * which only keeps more files.
   */
  public static VariantFilePruner of(Schema scanSchema, Expression filter, boolean caseSensitive) {
    Predicate<String> isVariantColumn = column -> {
      Types.NestedField field = caseSensitive ?
          scanSchema.findField(column) : scanSchema.caseInsensitiveFindField(column);
      return field != null && field.type().typeId() == Type.TypeID.VARIANT;
    };
    Expression extractFilter = extractConjuncts(filter, isVariantColumn);
    if (extractFilter == Expressions.alwaysTrue()) {
      return null;
    }
    return new VariantFilePruner(scanSchema, extractFilter, caseSensitive);
  }

  private static Expression extractConjuncts(Expression expr, Predicate<String> isVariantColumn) {
    if (expr instanceof And and) {
      return Expressions.and(
          extractConjuncts(and.left(), isVariantColumn), extractConjuncts(and.right(), isVariantColumn));
    }
    if (isExtractPredicate(expr, isVariantColumn) ||
        expr instanceof Not not && isExtractPredicate(not.child(), isVariantColumn)) {
      return expr;
    }
    return Expressions.alwaysTrue();
  }

  private static boolean isExtractPredicate(Expression expr, Predicate<String> isVariantColumn) {
    return expr instanceof UnboundPredicate<?> predicate && predicate.term() instanceof UnboundExtract<?> extract &&
        isVariantColumn.test(extract.ref().name());
  }

  /**
   * The variant columns whose stats {@link #mayMatch(FileScanTask)} needs; planned files carry no stats unless
   * the scan requests them.
   */
  public List<String> variantColumns() {
    return variantColumns;
  }

  /**
   * Returns false if no row of the task's file can match.
   */
  public boolean mayMatch(FileScanTask task) {
    DataFile file = task.file();
    if (!hasVariantBounds(file.lowerBounds()) && !hasVariantBounds(file.upperBounds())) {
      return evaluator.eval(file);
    }
    DataFile withVariantBounds = DataFiles.builder(task.spec())
        .copy(file)
        .withMetrics(new Metrics(file.recordCount(), file.columnSizes(), file.valueCounts(),
            file.nullValueCounts(), file.nanValueCounts(),
            littleEndian(file.lowerBounds()), littleEndian(file.upperBounds())))
        .build();
    return evaluator.eval(withVariantBounds);
  }

  private boolean hasVariantBounds(Map<Integer, ByteBuffer> bounds) {
    return bounds != null && variantFieldIds.stream().anyMatch(bounds::containsKey);
  }

  /**
   * Serialized variants are little-endian; a ByteBuffer's byte order defaults to big-endian.
   */
  private Map<Integer, ByteBuffer> littleEndian(Map<Integer, ByteBuffer> bounds) {
    if (bounds == null) {
      return null;
    }
    Map<Integer, ByteBuffer> view = Maps.newHashMap(bounds);
    variantFieldIds.forEach(id ->
        view.computeIfPresent(id, (key, buffer) -> buffer.duplicate().order(ByteOrder.LITTLE_ENDIAN)));
    return view;
  }
}
