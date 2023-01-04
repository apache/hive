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

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.parse.TransformSpec.TransformType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class PartitionTransform {

  private static final Map<Integer, TransformType> TRANSFORMS = Stream
      .of(new Object[][] { { HiveParser.TOK_IDENTITY, TransformType.IDENTITY },
          { HiveParser.TOK_YEAR, TransformType.YEAR },
          { HiveParser.TOK_MONTH, TransformType.MONTH },
          { HiveParser.TOK_DAY, TransformType.DAY },
          { HiveParser.TOK_HOUR, TransformType.HOUR },
          { HiveParser.TOK_TRUNCATE, TransformType.TRUNCATE },
          { HiveParser.TOK_BUCKET, TransformType.BUCKET } })
      .collect(Collectors.toMap(e -> (Integer) e[0], e -> (TransformType) e[1]));

  /**
   * Get the identity transform specification based on the partition columns
   * @param fields The partition column fields
   * @return list of partition transforms
   */
  public static List<TransformSpec> getPartitionTransformSpec(List<FieldSchema> fields) {
    return fields.stream()
               .map(field -> new TransformSpec(field.getName(), TransformType.IDENTITY, Optional.empty()))
               .collect(Collectors.toList());
  }

  /**
   * Parse the partition transform specifications from the AST Tree node.
   * @param node AST Tree node, must be not null
   * @return list of partition transforms
   */
  public static List<TransformSpec> getPartitionTransformSpec(ASTNode node) {
    List<TransformSpec> partSpecList = new ArrayList<>();
    for (int i = 0; i < node.getChildCount(); i++) {
      TransformSpec spec = new TransformSpec();
      ASTNode child = (ASTNode) node.getChild(i);
      for (int j = 0; j < child.getChildCount(); j++) {
        ASTNode grandChild = (ASTNode) child.getChild(j);
        switch (grandChild.getToken().getType()) {
          case HiveParser.TOK_IDENTITY:
          case HiveParser.TOK_YEAR:
          case HiveParser.TOK_MONTH:
          case HiveParser.TOK_DAY:
          case HiveParser.TOK_HOUR:
            spec.setColumnName(grandChild.getChild(0).getText().toLowerCase());
            spec.setTransformType(TRANSFORMS.get(grandChild.getToken().getType()));
            break;
          case HiveParser.TOK_TRUNCATE:
          case HiveParser.TOK_BUCKET:
            spec.setTransformType(TRANSFORMS.get(grandChild.getToken().getType()));
            spec.setTransformParam(Optional.ofNullable(Integer.valueOf(grandChild.getChild(0).getText())));
            spec.setColumnName(grandChild.getChild(1).getText().toLowerCase());
            break;
        }
      }
      partSpecList.add(spec);
    }

    return partSpecList;
  }

}
