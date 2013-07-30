package org.apache.hadoop.hive.metastore.model;

public class MDelegationToken {

  private String tokenStr;
  private String tokenIdentifier;

  public MDelegationToken(String tokenIdentifier, String tokenStr) {
    super();
    this.tokenStr = tokenStr;
    this.tokenIdentifier = tokenIdentifier;
  }

  public String getTokenStr() {
    return tokenStr;
  }
  public void setTokenStr(String tokenStr) {
    this.tokenStr = tokenStr;
  }
  public String getTokenIdentifier() {
    return tokenIdentifier;
  }
  public void setTokenIdentifier(String tokenIdentifier) {
    this.tokenIdentifier = tokenIdentifier;
  }

}
