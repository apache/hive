package org.apache.hadoop.hive.ql.optimizer.optiq;

public class Pair<T1, T2> {
  private final T1 m_first;
  private final T2 m_second;

  public Pair(T1 first, T2 second) {
    m_first = first;
    m_second = second;
  }

  public T1 getFirst() {
    return m_first;
  }

  public T2 getSecond() {
    return m_second;
  }
}
