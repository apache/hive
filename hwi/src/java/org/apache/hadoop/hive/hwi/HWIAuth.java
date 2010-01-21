package org.apache.hadoop.hive.hwi;

/**
 * Represents an authenticated user. This class is stored in the users session.
 * It is also used as a key for the HiveSessionManager
 */
public class HWIAuth implements Comparable {
  private String user;
  private String[] groups;

  public HWIAuth() {

  }

  public String getUser() {
    return user;
  }

  public void setUser(String user) {
    this.user = user;
  }

  public String[] getGroups() {
    return groups;
  }

  public void setGroups(String[] groups) {
    this.groups = groups;
  }

  /**
   * HWIAuth is used in SortedSets(s) the compartTo method is required
   * 
   * @return chained call to String.compareTo based on user property
   */
  public int compareTo(Object obj) {
    if (obj == null) {
      return -1;
    }
    if (!(obj instanceof HWIAuth)) {
      return -1;
    }
    HWIAuth o = (HWIAuth) obj;
    return o.getUser().compareTo(user);
  }

  /**
   * HWIAuth is used in Map(s) the hashCode method is required
   * 
   * @see java.lang.Object#hashCode()
   */
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((user == null) ? 0 : user.hashCode());
    return result;
  }

  /**
   * HWIAuth is used in Map(s) the equals method is required
   * 
   * @see java.lang.Object#equals(java.lang.Object)
   */
  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof HWIAuth)) {
      return false;
    }
    HWIAuth other = (HWIAuth) obj;
    if (user == null) {
      if (other.user != null) {
        return false;
      }
    } else if (!user.equals(other.user)) {
      return false;
    }
    return true;
  }

}