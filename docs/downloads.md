---
title: Downloads
layout: default
---

<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License. -->

Releases may be downloaded from Apache mirrors:

__[Download a release now!][HIVE_DL]__


On the mirror, all recent releases are available, but are not
guaranteed to be stable. For stable releases, look in the stable
directory.


## News
### 18 April 2020: release 2.3.7 available
This release works with Hadoop 2.x.y
You can look at the complete [JIRA change log for this release][HIVE_2_3_7_CL].

### 26 August 2019: release 3.1.2 available
This release works with Hadoop 3.x.y.
You can look at the complete [JIRA change log for this release][HIVE_3_1_2_CL].

### 23 August 2019: release 2.3.6 available
This release works with Hadoop 2.x.y.
You can look at the complete [JIRA change log for this release][HIVE_2_3_6_CL].

### 14 May 2019: release 2.3.5 available
This release works with Hadoop 2.x.y.
You can look at the complete [JIRA change log for this release][HIVE_2_3_5_CL].

### 7 November 2018: release 2.3.4 available
This release works with Hadoop 2.x.y.
You can look at the complete [JIRA change log for this release][HIVE_2_3_4_CL].

### 1 November 2018: release 3.1.1 available
This release works with Hadoop 3.x.y.
You can look at the complete [JIRA change log for this release][HIVE_3_1_1_CL].

### 30 July 2018: release 3.1.0 available
This release works with Hadoop 3.x.y.
You can look at the complete [JIRA change log for this release][HIVE_3_1_0_CL].

### 21 May 2018 : release 3.0.0 available
This release works with Hadoop 3.x.y.
The on-disk layout of Acid tables has changed with this release. Any Acid table partition that had Update/Delete/Merge statement executed since the last Major compaction must execute Major compaction before upgrading to 3.0.  No more Update/Delete/Merge may be executed against these tables since the start of Major compaction.  Not following this may lead to data corruption.  Tables/partitions that only contain results of Insert statements are fully compatible and don't need to be compacted.
You can look at the complete [JIRA change log for this release][HIVE_3_0_0_CL].

### 3 April 2018 : release 2.3.3 available
This release works with Hadoop 2.x.y
You can look at the complete [JIRA change log for this release][HIVE_2_3_3_CL].

### 18 November 2017 : release 2.3.2 available
This release works with Hadoop 2.x.y
You can look at the complete [JIRA change log for this release][HIVE_2_3_2_CL].

### 24 October 2017 : release 2.3.1 available
This release works with Hadoop 2.x.y
You can look at the complete [JIRA change log for this release][HIVE_2_3_1_CL].

### 25 July 2017 : release 2.2.0 available
This release works with Hadoop 2.x.y
You can look at the complete [JIRA change log for this release][HIVE_2_2_0_CL].

### 17 July 2017 : release 2.3.0 available
This release works with Hadoop 2.x.y
You can look at the complete [JIRA change log for this release][HIVE_2_3_0_CL].

### 07 April 2017 : release 1.2.2 available
This release works with Hadoop 1.x.y, 2.x.y
You can look at the complete [JIRA change log for this release][HIVE_1_2_2_CL].

### 8 December 2016 : release 2.1.1 available
This release works with Hadoop 2.x.y.
Hive 1.x line will continue to be maintained with Hadoop 1.x.y support.
You can look at the complete [JIRA change log for this release][HIVE_2_1_1_CL].

### 20 June 2016 : release 2.1.0 available
This release works with Hadoop 2.x.y.
Hive 1.x line will continue to be maintained with Hadoop 1.x.y support.
You can look at the complete [JIRA change log for this release][HIVE_2_1_0_CL].

### 25 May 2016 : release 2.0.1 available
This release works with Hadoop 2.x.y.
Hive 1.x line will continue to be maintained with Hadoop 1.x.y support.
You can look at the complete [JIRA change log for this release][HIVE_2_0_1_CL].

### 15 February 2016 : release 2.0.0 available
This release works with Hadoop 2.x.y.
Hive 1.x line will continue to be maintained with Hadoop 1.x.y support.
You can look at the complete [JIRA change log for this release][HIVE_2_0_0_CL].

### 28 Jan 2016 : hive-parent-auth-hook made available
This is a hook usable with hive to fix an authorization issue. Users
of Hive 1.0.x,1.1.x and 1.2.x are encouraged to use this hook. More
details can be found in the README inside the tar.gz file.

### 27 June 2015 : release 1.2.1 available
This release works with Hadoop 1.x.y, 2.x.y

You can look at the complete [JIRA change log for this release][HIVE_1_2_1_CL].

### 21 May 2015 : release 1.0.1, 1.1.1, and ldap-fix are available
These two releases works with Hadoop 1.x.y, 2.x.y. They are based on
Hive 1.0.0 and 1.1.0 respectively, plus a fix for a LDAP vulnerability issue.
Hive users for these two versions are encouraged to upgrade. Users
of previous versions can download and use the ldap-fix.
More details can be found in the README attached to the tar.gz file.

You can look at the complete JIRA change log for [release 1.0.1][HIVE_1_0_1_CL]
and [release 1.1.1][HIVE_1_1_1_CL]

### 18 May 2015 : release 1.2.0 available
This release works with Hadoop 1.x.y, 2.x.y

You can look at the complete [JIRA change log for this release][HIVE_1_2_0_CL].

### 8 March 2015: release 1.1.0 available
This release works with Hadoop 1.x.y, 2.x.y

You can look at the complete [JIRA change log for this release][HIVE_1_1_0_CL].

### 4 February 2015: release 1.0.0 available
This release works with Hadoop 1.x.y, 2.x.y

You can look at the complete [JIRA change log for this release][HIVE_1_0_0_CL].

### 12 November, 2014: release 0.14.0 available
This release works with Hadoop 1.x.y, 2.x.y

You can look at the complete [JIRA change log for this release][HIVE_14_CL].

### 6 June, 2014: release 0.13.1 available
This release works with Hadoop 0.20.x, 0.23.x.y, 1.x.y, 2.x.y

You can look at the complete [JIRA change log for this release][HIVE_13_1_CL].

### 21 April, 2014: release 0.13.0 available
This release  works with Hadoop 0.20.x, 0.23.x.y, 1.x.y, 2.x.y

You can look at the complete [JIRA change log for this release][HIVE_13_CL].

### 15 October, 2013: release 0.12.0 available
This release  works with Hadoop 0.20.x, 0.23.x.y, 1.x.y, 2.x.y

You can look at the complete [JIRA change log for this release][HIVE_12_CL].

### 15 May, 2013: release 0.11.0 available
This release  works with Hadoop 0.20.x, 0.23.x.y, 1.x.y, 2.x.y

You can look at the complete [JIRA change log for this release][HIVE_11_CL].

### March, 2013: HCatalog merges into Hive
Old HCatalog releases may still be [downloaded][HCAT_DL].

### 11 January, 2013: release 0.10.0 available
This release  works with Hadoop 0.20.x, 0.23.x.y, 1.x.y, 2.x.y

You can look at the complete [JIRA change log for this release][HIVE_10_CL].

[HIVE_DL]: http://www.apache.org/dyn/closer.cgi/hive/
[HIVE_3_1_2_CL]: https://issues.apache.org/jira/secure/ReleaseNote.jspa?version=12344397&styleName=Html&projectId=12310843
[HIVE_2_3_7_CL]: https://issues.apache.org/jira/secure/ReleaseNote.jspa?version=12346056&styleName=Text&projectId=12310843
[HIVE_2_3_6_CL]: https://issues.apache.org/jira/secure/ReleaseNote.jspa?version=12345603&styleName=Text&projectId=12310843
[HIVE_2_3_5_CL]: https://issues.apache.org/jira/secure/ReleaseNote.jspa?version=12345394&styleName=Text&projectId=12310843
[HIVE_2_3_4_CL]: https://issues.apache.org/jira/secure/ReleaseNote.jspa?version=12344319&styleName=Text&projectId=12310843
[HIVE_3_1_1_CL]: https://issues.apache.org/jira/secure/ReleaseNote.jspa?version=12344240&styleName=Text&projectId=12310843
[HIVE_3_1_0_CL]: https://issues.apache.org/jira/secure/ReleaseNote.jspa?version=12343014&styleName=Text&projectId=12310843
[HIVE_3_0_0_CL]: https://issues.apache.org/jira/secure/ReleaseNote.jspa?version=12340268&styleName=Text&projectId=12310843
[HIVE_2_3_3_CL]: https://issues.apache.org/jira/secure/ReleaseNote.jspa?version=12342162&styleName=Text&projectId=12310843
[HIVE_2_3_2_CL]: https://issues.apache.org/jira/secure/ReleaseNote.jspa?version=12342053&styleName=Text&projectId=12310843
[HIVE_2_3_1_CL]: https://issues.apache.org/jira/secure/ReleaseNote.jspa?version=12341418&styleName=Text&projectId=12310843
[HIVE_2_2_0_CL]: https://issues.apache.org/jira/secure/ReleaseNote.jspa?version=12335837&styleName=Text&projectId=12310843
[HIVE_2_3_0_CL]: https://issues.apache.org/jira/secure/ReleaseNote.jspa?version=12340269&styleName=Text&projectId=12310843
[HIVE_2_1_1_CL]: https://issues.apache.org/jira/secure/ReleaseNote.jspa?version=12335838&styleName=Text&projectId=12310843
[HIVE_2_1_0_CL]: https://issues.apache.org/jira/secure/ReleaseNote.jspa?version=12334255&styleName=Text&projectId=12310843
[HIVE_2_0_1_CL]: https://issues.apache.org/jira/secure/ReleaseNote.jspa?version=12334886&styleName=Text&projectId=12310843
[HIVE_2_0_0_CL]: https://issues.apache.org/jira/secure/ReleaseNote.jspa?version=12332641&styleName=Text&projectId=12310843
[HIVE_1_2_1_CL]: https://issues.apache.org/jira/secure/ReleaseNote.jspa?version=12332384&styleName=Text&projectId=12310843
[HIVE_1_2_2_CL]:https://issues.apache.org/jira/secure/ReleaseNote.jspa?version=12332952&styleName=Text&projectId=12310843
[HIVE_1_1_1_CL]: https://issues.apache.org/jira/secure/ReleaseNote.jspa?version=12329557&styleName=Text&projectId=12310843
[HIVE_1_0_1_CL]: https://issues.apache.org/jira/secure/ReleaseNote.jspa?version=12329444&styleName=Text&projectId=12310843
[HIVE_1_2_0_CL]: https://issues.apache.org/jira/secure/ReleaseNote.jspa?version=12329345&styleName=Text&projectId=12310843
[HIVE_1_1_0_CL]: https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12310843&styleName=Text&version=12329363
[HIVE_1_0_0_CL]: https://issues.apache.org/jira/secure/ReleaseNote.jspa?version=12329278&styleName=Text&projectId=12310843
[HIVE_14_CL]: https://issues.apache.org/jira/secure/ReleaseNote.jspa?version=12326450&styleName=Text&projectId=12310843
[HIVE_13_1_CL]: https://issues.apache.org/jira/secure/ReleaseNote.jspa?version=12326829&styleName=Text&projectId=12310843
[HIVE_13_CL]: https://issues.apache.org/jira/secure/ReleaseNote.jspa?version=12324986&styleName=Text&projectId=12310843
[HIVE_12_CL]: https://issues.apache.org/jira/secure/ReleaseNote.jspa?version=12324312&styleName=Text&projectId=12310843
[HIVE_11_CL]: https://issues.apache.org/jira/secure/ReleaseNote.jspa?version=12323587&styleName=Text&projectId=12310843
[HCAT_DL]: /hcatalog_downloads.html
[HIVE_10_CL]: https://issues.apache.org/jira/secure/ReleaseNote.jspa?version=12320745&styleName=Text&projectId=12310843
