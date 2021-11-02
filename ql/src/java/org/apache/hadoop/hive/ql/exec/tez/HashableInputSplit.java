package org.apache.hadoop.hive.ql.exec.tez;

/**
 * An InputSplit type that has a custom/specific way of producing its serialized content, to be later used as input
 * of a hash function. Used for LLAP cache affinity, so that a certain split always ends up on the same executor.
 */
public interface HashableInputSplit {

  byte[] getBytesForHash();

}
