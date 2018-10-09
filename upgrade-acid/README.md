Please note that this module in HDP doesn't match the same module in Apache.

In HDP we support upgrade from HDP 2.6.x to 3.x.  HDP 2.6 ships with both Hive1 and Hive2 but
the metastore component used by both is using Hive1 codebase.  The upgrade logic needs to interact
with the metastore.  This Hive1 codebase is far
ahead of the 1.x release in Apache.  There is no active work on Hive1 in Apache - the last
1.x release was in March of 2017.  The Acid Upgrade module in Apache is build to upgrade
Hive2 to Hive3.  Hive1 and Hive2 have diverged too far in both API and dependencies to 
make a simple shim layer work.  Same is true for HDP Hive1 and ASF Hive1.
