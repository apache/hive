package org.apache.hadoop.hive.metastore.model;

public class MMasterKey {

  public MMasterKey(int keyId, String masterKey) {
    this.keyId = keyId;
    this.masterKey = masterKey;
  }

  private int keyId;
  private String masterKey;

  public MMasterKey(String masterKey) {
    this.masterKey = masterKey;
  }

  public MMasterKey(int keyId) {
    this.keyId = keyId;
  }

  public String getMasterKey() {
    return masterKey;
  }

  public void setMasterKey(String masterKey) {
    this.masterKey = masterKey;
  }

  public int getKeyId() {
    return keyId;
  }

  public void setKeyId(int keyId) {
    this.keyId = keyId;
  }

}
