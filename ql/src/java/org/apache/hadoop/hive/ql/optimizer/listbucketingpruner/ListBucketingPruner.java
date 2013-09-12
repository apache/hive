/**
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

package org.apache.hadoop.hive.ql.optimizer.listbucketingpruner;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.optimizer.PrunerUtils;
import org.apache.hadoop.hive.ql.optimizer.Transform;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.PrunedPartitionList;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;

/**
 * The transformation step that does list bucketing pruning.
 *
 */
public class ListBucketingPruner implements Transform {
  static final Log LOG = LogFactory.getLog(ListBucketingPruner.class.getName());

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hadoop.hive.ql.optimizer.Transform#transform(org.apache.hadoop.hive.ql.parse.
   * ParseContext)
   */
  @Override
  public ParseContext transform(ParseContext pctx) throws SemanticException {
    // create a the context for walking operators
    NodeProcessorCtx opPartWalkerCtx = new LBOpPartitionWalkerCtx(pctx);

    // Retrieve all partitions generated from partition pruner and partition column pruner
    PrunerUtils.walkOperatorTree(pctx, opPartWalkerCtx, LBPartitionProcFactory.getFilterProc(),
        LBPartitionProcFactory.getDefaultProc());

    PrunedPartitionList partsList = ((LBOpPartitionWalkerCtx) opPartWalkerCtx).getPartitions();
    if (partsList != null) {
      Set<Partition> parts = partsList.getPartitions();
      if ((parts != null) && (parts.size() > 0)) {
        for (Partition part : parts) {
          // only process partition which is skewed and list bucketed
          if (ListBucketingPrunerUtils.isListBucketingPart(part)) {
            // create a the context for walking operators
            NodeProcessorCtx opWalkerCtx = new LBOpWalkerCtx(pctx.getOpToPartToSkewedPruner(),
                part);

            // walk operator tree to create expression tree for list bucketing
            PrunerUtils.walkOperatorTree(pctx, opWalkerCtx, LBProcFactory.getFilterProc(),
                LBProcFactory.getDefaultProc());
          }
        }
      }
    }

    return pctx;
  }

  /**
   * Prunes to the directories which match the skewed keys in where clause.
   *
   *
   * Algorithm
   *
   * =========
   *
   * For each possible skewed element combination:
   * 1. walk through ExprNode tree
   * 2. decide Boolean (True/False/unknown(null))
   *
   * Go through each skewed element combination again:
   * 1. if it is skewed value, skip the directory only if it is false, otherwise keep it
   * 2. skip the default directory only if all skewed elements,non-skewed value, are false.
   *
   * Example
   * =======
   * For example:
   * 1. skewed column (list): C1, C2
   * 2. skewed value (list of list): (1,a), (2,b), (1,c)
   *
   * Unique skewed elements for each skewed column (list of list):
   * (1,2,other), (a,b,c,other)
   *
   * Index: (0,1,2) (0,1,2,3)
   * Output matches order of skewed column. Output can be read as:
   *
   * C1 has unique element list (1,2,other)
   * C2 has unique element list (a,b,c,other)
   *
   * C1\C2 | a | b | c |Other
   * 1 | (1,a) | X | (1,c) |X
   * 2 | X |(2,b) | X |X
   * other | X | X | X |X
   *
   * Complete dynamic-multi-dimension collection
   *
   * (0,0) (1,a) * -> T
   * (0,1) (1,b) -> T
   * (0,2) (1,c) *-> F
   * (0,3) (1,other)-> F
   * (1,0) (2,a)-> F
   * (1,1) (2,b) * -> T
   * (1,2) (2,c)-> F
   * (1,3) (2,other)-> F
   * (2,0) (other,a) -> T
   * (2,1) (other,b) -> T
   * (2,2) (other,c) -> T
   * (2,3) (other,other) -> T
   * * is skewed value entry
   *
   * Expression Tree : ((c1=1) and (c2=a)) or ( (c1=3) or (c2=b))
   *
   * or
   * / \
   * and or
   * / \ / \
   * c1=1 c2=a c1=3 c2=b
   *
   *
   * For each entry in dynamic-multi-dimension container
   *
   * 1. walk through the tree to decide value (please see map's value above)
   * 2. if it is skewed value
   * 2.1 remove the entry from the map
   * 2.2 add directory to path unless value is false
   * 3. otherwise, add value to map
   *
   * Once it is done, go through the rest entries in map to decide default directory
   * 1. we know all is not skewed value
   * 2. we skip default directory only if all value is false
   *
   * What we choose at the end?
   *
   * 1. directory for (1,a) because it 's skewed value and match returns true
   * 2. directory for (2,b) because it 's skewed value and match returns true
   * 3. default directory because not all non-skewed value returns false
   *
   * we skip directory for (1,c) since match returns false
   *
   * Note: unknown is marked in {@link #transform(ParseContext)} <blockquote>
   * <pre>
   * newcd = new ExprNodeConstantDesc(cd.getTypeInfo(), null)
   * </pre>
   *
   * </blockquote> can be checked via <blockquote>
   *
   * <pre>
   *     child_nd instanceof ExprNodeConstantDesc
   *               && ((ExprNodeConstantDesc) child_nd).getValue() == null)
   * </pre>
   *
   * </blockquote>
   *
   * @param ctx
   *          parse context
   * @param part
   *          partition
   * @param pruner
   *          expression node tree
   * @return
   */
  public static Path[] prune(ParseContext ctx, Partition part, ExprNodeDesc pruner) {
    Path[] finalPaths = null;

    try {
      finalPaths = execute(ctx, part, pruner);
    } catch (SemanticException e) {
      // Use full partition path for error case.
      LOG.warn("Using full partition scan :" + part.getPath() + ".", e);
      finalPaths = part.getPath();
    }

    return finalPaths;
  }

  /**
   * Main skeleton for list bucketing pruning.
   *
   * @param ctx
   * @param part
   * @param pruner
   * @return
   * @throws SemanticException
   */
  private static Path[] execute(ParseContext ctx, Partition part, ExprNodeDesc pruner)
      throws SemanticException {
    Path[] finalPaths;

    List<Path> selectedPaths = new ArrayList<Path>();

    if (ListBucketingPrunerUtils.isUnknownState(pruner)) {
      // Use full partition path for error case.
      LOG.warn("List bucketing pruner is either null or in unknown state "
          + " so that it uses full partition scan :" + part.getPath());
      finalPaths = part.getPath();
    } else {
      // Retrieve skewed columns.
      List<List<String>> sVals = part.getSkewedColValues();

      assert ((sVals != null) && (sVals.size() > 0)) :
        part.getName() + " skewed metadata is corrupted. No skewed value information.";
      // Calculate collection.
      List<List<String>> indexCollection = DynamicMultiDimensionalCollection
          .generateCollection(sVals);

      assert (indexCollection != null) : "Collection is null.";

      // Calculate unique skewed elements for each skewed column.
      List<List<String>> uniqSkewValues = DynamicMultiDimensionalCollection.uniqueSkewedValueList(
          sVals);
      // Decide skewed value directory selection.
      List<Boolean> nonSkewedValueMatchResult = decideSkewedValueDirSelection(part, pruner,
          selectedPaths, indexCollection, uniqSkewValues);

      // Decide default directory selection.
      decideDefaultDirSelection(part, selectedPaths, nonSkewedValueMatchResult);

      // Finalize paths.
      finalPaths = generateFinalPath(part, selectedPaths);

    }
    return finalPaths;
  }

  /**
   * Walk through every entry in complete collection
   * 1. calculate if it matches expression tree
   * 2. decide if select skewed value directory
   * 3. store match result for non-skewed value for later handle on default directory
   * C1\C2 | a | b | c |Other
   * 1 | (1,a) | X | (1,c) |X
   * 2 | X |(2,b) | X |X
   * other | X | X | X |X
   * Final result
   * Complete dynamic-multi-dimension collection
   * (0,0) (1,a) * -> T
   * (0,1) (1,b) -> T
   * (0,2) (1,c) *-> F
   * (0,3) (1,other)-> F
   * (1,0) (2,a)-> F
   * (1,1) (2,b) * -> T
   * (1,2) (2,c)-> F
   * (1,3) (2,other)-> F
   * (2,0) (other,a) -> T
   * (2,1) (other,b) -> T
   * (2,2) (other,c) -> T
   * (2,3) (other,other) -> T
   *
   * * is skewed value entry
   *
   * 1. directory for (1,a) is chosen because it 's skewed value and match returns true
   * 2. directory for (2,b) is chosen because it 's skewed value and match returns true
   *
   * @param part
   * @param pruner
   * @param selectedPaths
   * @param collections
   * @param uniqSkewedValues
   * @return
   * @throws SemanticException
   */
  private static List<Boolean> decideSkewedValueDirSelection(Partition part, ExprNodeDesc pruner,
      List<Path> selectedPaths, List<List<String>> collections,
      List<List<String>> uniqSkewedValues) throws SemanticException {
    // For each entry in dynamic-multi-dimension collection.
    List<String> skewedCols = part.getSkewedColNames(); // Retrieve skewed column.
    Map<List<String>, String> mappings = part.getSkewedColValueLocationMaps(); // Retrieve skewed
                                                                               // map.
    assert ListBucketingPrunerUtils.isListBucketingPart(part) : part.getName()
        + " skewed metadata is corrupted. No skewed column and/or location mappings information.";
    List<List<String>> skewedValues = part.getSkewedColValues();
    List<Boolean> nonSkewedValueMatchResult = new ArrayList<Boolean>();
    for (List<String> cell : collections) {
      // Walk through the tree to decide value.
      // Example: skewed column: C1, C2 ;
      // index: (1,a) ;
      // expression tree: ((c1=1) and (c2=a)) or ((c1=3) or (c2=b))
      Boolean matchResult = ListBucketingPrunerUtils.evaluateExprOnCell(skewedCols, cell, pruner,
          uniqSkewedValues);
      // Handle skewed value.
      if (skewedValues.contains(cell)) { // if it is skewed value
        if ((matchResult == null) || matchResult) { // add directory to path unless value is false
          /* It's valid case if a partition: */
          /* 1. is defined with skewed columns and skewed values in metadata */
          /* 2. doesn't have all skewed values within its data */
          if (mappings.get(cell) != null) {
            selectedPaths.add(new Path(mappings.get(cell)));
          }
        }
      } else {
        // Non-skewed value, add it to list for later handle on default directory.
        nonSkewedValueMatchResult.add(matchResult);
      }
    }
    return nonSkewedValueMatchResult;
  }

  /**
   * Decide whether should select the default directory.
   *
   * @param part
   * @param selectedPaths
   * @param nonSkewedValueMatchResult
   */
  private static void decideDefaultDirSelection(Partition part, List<Path> selectedPaths,
      List<Boolean> nonSkewedValueMatchResult) {
    boolean skipDefDir = true;
    for (Boolean v : nonSkewedValueMatchResult) {
      if ((v == null) || v) {
        skipDefDir = false; // we skip default directory only if all value is false
        break;
      }
    }
    if (!skipDefDir) {
      StringBuilder builder = new StringBuilder();
      builder.append(part.getLocation());
      builder.append(Path.SEPARATOR);
      builder
          .append((FileUtils.makeDefaultListBucketingDirName(
              part.getSkewedColNames(),
              ListBucketingPrunerUtils.HIVE_LIST_BUCKETING_DEFAULT_DIR_NAME)));
      selectedPaths.add(new Path(builder.toString()));
    }
  }

  /**
   * Decide the final path.
   *
   * @param part
   * @param selectedPaths
   * @return
   */
  private static Path[] generateFinalPath(Partition part, List<Path> selectedPaths) {
    Path[] finalPaths;
    if (selectedPaths.size() == 0) {
      LOG.warn("Using full partition scan :" + part.getPath() + ".");
      finalPaths = part.getPath();
    } else {
      finalPaths = selectedPaths.toArray(new Path[0]);
    }
    return finalPaths;
  }


  /**
   * Note: this class is not designed to be used in general but for list bucketing pruner only.
   * The structure addresses the following requirements:
   * 1. multiple dimension collection
   * 2. length of each dimension is dynamic. It's decided at runtime.
   * The first user is list bucketing pruner and used in pruning phase:
   * 1. Each skewed column has a batch of skewed elements.
   * 2. One skewed column represents one dimension.
   * 3. Length of dimension is size of skewed elements.
   * 4. no. of skewed columns and length of dimension are dynamic and configured by user.
   * use case:
   * ========
   * Use case #1:
   * Multiple dimension collection represents if to select a directory representing by the cell.
   * skewed column: C1, C2, C3
   * skewed value: (1,a,x), (2,b,x), (1,c,x), (2,a,y)
   * Other: represent value for the column which is not part of skewed value.
   * C3 = x
   * C1\C2 | a | b | c |Other
   * 1 | Boolean(1,a,x) | X | Boolean(1,c,x) |X
   * 2 | X |Boolean(2,b,x) | X |X
   * other | X | X | X |X
   * C3 = y
   * C1\C2 | a | b | c |Other
   * 1 | X | X | X |X
   * 2 | Boolean(2,a,y) | X | X |X
   * other | X | X | X |X
   * Boolean is cell type which can be False/True/Null(Unknown).
   * (1,a,x) is just for information purpose to explain which skewed value it represents.
   * 1. value of Boolean(1,a,x) represents if we select the directory for list bucketing
   * 2. value of Boolean(2,b,x) represents if we select the directory for list bucketing
   * ...
   * 3. All the rest, marked as "X", will decide if to pickup the default directory.
   * 4. Not only "other" columns/rows but also the rest as long as it doesn't represent skewed
   * value.
   * For cell representing skewed value:
   * 1. False, skip the directory
   * 2. True/Unknown, select the directory
   * For cells representing default directory:
   * 1. only if all cells are false, skip the directory
   * 2. all other cases, select the directory
   * Use case #2:
   * Multiple dimension collection represents skewed elements so that walk through tree one by one.
   * Cell is a List<String> representing the value mapping from index path and skewed value.
   * skewed column: C1, C2, C3
   * skewed value: (1,a,x), (2,b,x), (1,c,x), (2,a,y)
   * Other: represent value for the column which is not part of skewed value.
   * C3 = x
   * C1\C2 | a | b | c |Other
   * 1 | (1,a,x) | X | (1,c,x) |X
   * 2 | X |(2,b,x) | X |X
   * other | X | X | X |X
   * C3 = y
   * C1\C2 | a | b | c |Other
   * 1 | X | X | X |X
   * 2 | (2,a,y) | X | X |X
   * other | X | X | X |X
   * Implementation:
   * ==============
   * please see another example in {@link ListBucketingPruner#prune}
   * We will use a HasMap to represent the Dynamic-Multiple-Dimension collection:
   * 1. Key is List<Integer> representing the index path to the cell
   * 2. value represents the cell (Boolean for use case #1, List<String> for case #2)
   * For example:
   * 1. skewed column (list): C1, C2, C3
   * 2. skewed value (list of list): (1,a,x), (2,b,x), (1,c,x), (2,a,y)
   * From skewed value, we calculate the unique skewed element for each skewed column:
   * C1: (1,2)
   * C2: (a,b,c)
   * C3: (x,y)
   * We store them in list of list. We don't need to store skewed column name since we use order to
   * match:
   * 1. Skewed column (list): C1, C2, C3
   * 2. Unique skewed elements for each skewed column (list of list):
   * (1,2,other), (a,b,c,other), (x,y,other)
   * 3. index (0,1,2) (0,1,2,3) (0,1,2)
   *
   * We use the index,starting at 0. to construct hashmap representing dynamic-multi-dimension
   * collection:
   * key (what skewed value key represents) -> value (Boolean for use case #1, List<String> for case
   * #2).
   * (0,0,0) (1,a,x)
   * (0,0,1) (1,a,y)
   * (0,1,0) (1,b,x)
   * (0,1,1) (1,b,y)
   * (0,2,0) (1,c,x)
   * (0,2,1) (1,c,y)
   * (1,0,0) (2,a,x)
   * (1,0,1) (2,a,y)
   * (1,1,0) (2,b,x)
   * (1,1,1) (2,b,y)
   * (1,2,0) (2,c,x)
   * (1,2,1) (2,c,y)
   * ...
   */
  public static class DynamicMultiDimensionalCollection {

    /**
     * Find out complete skewed-element collection
     * For example:
     * 1. skewed column (list): C1, C2
     * 2. skewed value (list of list): (1,a), (2,b), (1,c)
     * It returns the complete collection
     * (1,a) , (1,b) , (1,c) , (1,other), (2,a), (2,b) , (2,c), (2,other), (other,a), (other,b),
     * (other,c), (other,other)
     * @throws SemanticException
     */
    public static List<List<String>> generateCollection(List<List<String>> values)
        throws SemanticException {
      // Calculate unique skewed elements for each skewed column.
      List<List<String>> uniqSkewedElements = DynamicMultiDimensionalCollection.uniqueElementsList(
          values, ListBucketingPrunerUtils.HIVE_LIST_BUCKETING_DEFAULT_KEY);
      // Calculate complete dynamic-multi-dimension collection.
      return DynamicMultiDimensionalCollection.flat(uniqSkewedElements);
    }

    /**
     * Convert value to unique element list. This is specific for skew value use case:
     * For example:
     * 1. skewed column (list): C1, C2, C3
     * 2. skewed value (list of list): (1,a,x), (2,b,x), (1,c,x), (2,a,y)
     * Input: skewed value (list of list): (1,a,x), (2,b,x), (1,c,x), (2,a,y)
     * Output: Unique skewed elements for each skewed column (list of list):
     * (1,2,other), (a,b,c,other), (x,y,other)
     * Output matches order of skewed column. Output can be read as:
     * C1 has unique element list (1,2,other)
     * C2 has unique element list (a,b,c,other)
     * C3 has unique element list (x,y,other)
     * Other represents any value which is not part skewed-value combination.
     * @param values
     *          skewed value list
     * @return a list of unique element lists
     */
    public static List<List<String>> uniqueElementsList(List<List<String>> values,
        String defaultDirName) {
      // Get unique skewed value list.
      List<List<String>> result = uniqueSkewedValueList(values);

      // Add default dir at the end of each list
      for (List<String> list : result) {
        list.add(defaultDirName);
      }

      return result;
    }

    /**
     * Convert value to unique skewed value list. It is used in
     * {@link ListBucketingPrunerUtils#evaluateExprOnCell}
     *
     * For example:
     *
     * 1. skewed column (list): C1, C2, C3
     * 2. skewed value (list of list): (1,a,x), (2,b,x), (1,c,x), (2,a,y)
     *
     * Input: skewed value (list of list): (1,a,x), (2,b,x), (1,c,x), (2,a,y)
     * Output: Unique skewed value for each skewed column (list of list):
     * (1,2), (a,b,c), (x,y)
     *
     * Output matches order of skewed column. Output can be read as:
     * C1 has unique skewed value list (1,2,)
     * C2 has unique skewed value list (a,b,c)
     * C3 has unique skewed value list (x,y)
     *
     * @param values
     *          skewed value list
     * @return a list of unique skewed value lists
     */
    public static List<List<String>> uniqueSkewedValueList(List<List<String>> values) {
      if ((values == null) || (values.size() == 0)) {
        return null;
      }

      // skewed value has the same length.
      List<List<String>> result = new ArrayList<List<String>>();
      for (int i = 0; i < values.get(0).size(); i++) {
        result.add(new ArrayList<String>());
      }

      // add unique element to list per occurrence order in skewed value.
      // occurrence order in skewed value doesn't matter.
      // as long as we add them to a list, order is preserved from now on.
      for (List<String> value : values) {
        for (int i = 0; i < value.size(); i++) {
          if (!result.get(i).contains(value.get(i))) {
            result.get(i).add(value.get(i));
          }
        }
      }

      return result;
    }

    /**
     * Flat a dynamic-multi-dimension collection.
     *
     * For example:
     * 1. skewed column (list): C1, C2, C3
     * 2. skewed value (list of list): (1,a,x), (2,b,x), (1,c,x), (2,a,y)
     *
     * Unique skewed elements for each skewed column (list of list):
     * (1,2,other), (a,b,c,other)
     * Index: (0,1,2) (0,1,2,3)
     *
     * Complete dynamic-multi-dimension collection
     * (0,0) (1,a) * -> T
     * (0,1) (1,b) -> T
     * (0,2) (1,c) *-> F
     * (0,3) (1,other)-> F
     * (1,0) (2,a)-> F
     * (1,1) (2,b) * -> T
     * (1,2) (2,c)-> F
     * (1,3) (2,other)-> F
     * (2,0) (other,a) -> T
     * (2,1) (other,b) -> T
     * (2,2) (other,c) -> T
     * (2,3) (other,other) -> T
     * * is skewed value entry
     *
     * @param uniqSkewedElements
     *
     * @return
     */
    public static List<List<String>> flat(List<List<String>> uniqSkewedElements)
        throws SemanticException {
      if (uniqSkewedElements == null) {
        return null;
      }
      List<List<String>> collection = new ArrayList<List<String>>();
      walker(collection, uniqSkewedElements, new ArrayList<String>(), 0);
      return collection;
    }

    /**
     * Flat the collection recursively.
     *
     * @param finalResult
     * @param input
     * @param listSoFar
     * @param level
     * @throws SemanticException
     */
    private static void walker(List<List<String>> finalResult, final List<List<String>> input,
        List<String> listSoFar, final int level) throws SemanticException {
      // Base case.
      if (level == (input.size() - 1)) {
        assert (input.get(level) != null) : "Unique skewed element list has null list in " + level
            + "th position.";
        for (String v : input.get(level)) {
          List<String> oneCompleteIndex = new ArrayList<String>(listSoFar);
          oneCompleteIndex.add(v);
          finalResult.add(oneCompleteIndex);
        }
        return;
      }

      // Recursive.
      for (String v : input.get(level)) {
        List<String> clonedListSoFar = new ArrayList<String>(listSoFar);
        clonedListSoFar.add(v);
        int nextLevel = level + 1;
        walker(finalResult, input, clonedListSoFar, nextLevel);
      }
    }

  }
}
