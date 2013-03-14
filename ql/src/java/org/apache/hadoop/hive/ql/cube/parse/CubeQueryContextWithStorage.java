package org.apache.hadoop.hive.ql.cube.parse;

import java.util.List;

import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.QB;
import org.apache.hadoop.hive.ql.parse.SemanticException;

public class CubeQueryContextWithStorage extends CubeQueryContext {

  private final List<String> supportedStorages;

  public CubeQueryContextWithStorage(ASTNode ast, QB qb,
      List<String> supportedStorages) throws SemanticException {
    super(ast, qb, null);
    this.supportedStorages = supportedStorages;
  }

  public CubeQueryContextWithStorage(CubeQueryContext cubeql,
      List<String> supportedStorages) {
    super(cubeql);
    this.supportedStorages = supportedStorages;
  }

  public List<String> getStorageNames() {
    return supportedStorages;
  }

}
