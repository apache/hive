namespace java org.apache.hadoop.hive.ql.plan.api
namespace cpp Hive

enum AdjacencyType { CONJUNCTIVE, DISJUNCTIVE }
struct Adjacency {
1: string node,
2: list<string> children,
3: AdjacencyType adjacencyType,
}

enum NodeType { OPERATOR, STAGE }
struct Graph {
1: NodeType nodeType,
2: list<string> roots,
3: list<Adjacency> adjacencyList,
}

#Represents a operator along with its counters
enum OperatorType { JOIN, MAPJOIN, EXTRACT, FILTER, FORWARD, GROUPBY, LIMIT, SCRIPT, SELECT, TABLESCAN, FILESINK, REDUCESINK, UNION, UDTF }
struct Operator {
1: string operatorId,
2: OperatorType operatorType,
3: map<string, string> operatorAttributes,
4: map<string, i64> operatorCounters,
5: bool done,
6: bool started,
}

# Represents whether it is a map-reduce job or not. In future, different tasks can add their dependencies
# The operator graph shows the operator tree
enum TaskType { MAP, REDUCE, OTHER }
struct Task {
1: string taskId,
2: TaskType taskType
3: map<string, string> taskAttributes,
4: map<string, i64> taskCounters,
5: optional Graph operatorGraph,
6: optional list<Operator> operatorList,
7: bool done,
8: bool started,
}

# Represents a Stage - unfortunately, it is represented as Task in ql/exec
enum StageType { CONDITIONAL, COPY, DDL, MAPRED, EXPLAIN, FETCH, FUNC, MAPREDLOCAL, MOVE }

struct Stage {
1: string stageId,
2: StageType stageType,
3: map<string, string> stageAttributes,
4: map<string, i64> stageCounters,
5: list<Task> taskList,
6: bool done,
7: bool started,
}

# Represents a query -
# The graph maintains the stage dependency.In case of conditional tasks, it is represented as if only
# one of the dependencies need to be executed
struct Query {
1: string queryId,
2: string queryType,
3: map<string, string> queryAttributes,
4: map<string, i64> queryCounters,
5: Graph stageGraph,
6: list<Stage> stageList,
7: bool done,
8: bool started,
}

# List of all queries - each query maintains if it is done or started
# This can be used to track all the queries in the session
struct QueryPlan {
1: list<Query> queries,
2: bool done,
3: bool started,
}
