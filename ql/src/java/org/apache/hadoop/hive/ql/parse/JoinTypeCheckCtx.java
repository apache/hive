package org.apache.hadoop.hive.ql.parse;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;

import com.google.common.collect.ImmutableList;

public class JoinTypeCheckCtx implements NodeProcessorCtx {
  /**
   * Potential typecheck error reason.
   */
  private String error;

  /**
   * The node that generated the potential typecheck error
   */
  private ASTNode errorSrcNode;

  private final ImmutableList<RowResolver> m_inputRRLst;

  public JoinTypeCheckCtx(RowResolver... inputRRLst) {
    m_inputRRLst = new ImmutableList.Builder<RowResolver>().addAll(Arrays.asList(inputRRLst)).build();
  }

  /**
   * @return the inputRR List
   */
  public List<RowResolver> getInputRRList() {
    return m_inputRRLst;
  }

  /**
   * @param error
   *          the error to set
   *
   */
  public void setError(String error, ASTNode errorSrcNode) {
    this.error = error;
    this.errorSrcNode = errorSrcNode;
  }

  /**
   * @return the error
   */
  public String getError() {
    return error;
  }

  public ASTNode getErrorSrcNode() {
    return errorSrcNode;
  } 
}
