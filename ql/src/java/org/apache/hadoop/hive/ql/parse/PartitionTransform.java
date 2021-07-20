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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class PartitionTransform {

  private static final Map<Integer, PartitionTransformSpec.TransformType> TRANSFORMS = Stream
      .of(new Object[][] { { HiveParser.TOK_IDENTITY, PartitionTransformSpec.TransformType.IDENTITY },
          { HiveParser.TOK_YEAR, PartitionTransformSpec.TransformType.YEAR },
          { HiveParser.TOK_MONTH, PartitionTransformSpec.TransformType.MONTH },
          { HiveParser.TOK_DAY, PartitionTransformSpec.TransformType.DAY },
          { HiveParser.TOK_HOUR, PartitionTransformSpec.TransformType.HOUR },
          { HiveParser.TOK_TRUNCATE, PartitionTransformSpec.TransformType.TRUNCATE },
          { HiveParser.TOK_BUCKET, PartitionTransformSpec.TransformType.BUCKET } })
      .collect(Collectors.toMap(e -> (Integer) e[0], e -> (PartitionTransformSpec.TransformType) e[1]));

  /**
   * Parse the partition transform specifications from the AST Tree node.
   * @param node AST Tree node, must be not null
   * @return list of partition transforms
   */
  public static List<PartitionTransformSpec> getPartitionTransformSpec(ASTNode node) {
    List<PartitionTransformSpec> partSpecList = new ArrayList<>();
    for (int i = 0; i < node.getChildCount(); i++) {
      PartitionTransformSpec spec = new PartitionTransformSpec();
      ASTNode child = (ASTNode) node.getChild(i);
      for (int j = 0; j < child.getChildCount(); j++) {
        ASTNode grandChild = (ASTNode) child.getChild(j);
        switch (grandChild.getToken().getType()) {
          case HiveParser.TOK_IDENTITY:
          case HiveParser.TOK_YEAR:
          case HiveParser.TOK_MONTH:
          case HiveParser.TOK_DAY:
          case HiveParser.TOK_HOUR:
            spec.setColumnName(grandChild.getChild(0).getText());
            spec.setTransformType(TRANSFORMS.get(grandChild.getToken().getType()));
            break;
          case HiveParser.TOK_TRUNCATE:
          case HiveParser.TOK_BUCKET:
            spec.setTransformType(TRANSFORMS.get(grandChild.getToken().getType()));
            spec.setTransformParam(Optional.ofNullable(Integer.valueOf(grandChild.getChild(0).getText())));
            spec.setColumnName(grandChild.getChild(1).getText());
            break;
        }
      }
      partSpecList.add(spec);
    }

    return partSpecList;
  }

}
