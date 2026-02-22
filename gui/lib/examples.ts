/** Comprehensive Datalog examples organized by category, extracted from 1,100+ snapshot tests. */

export interface Example {
  name: string
  description: string
  code: string
  difficulty: "beginner" | "intermediate" | "advanced"
}

export interface ExampleCategory {
  id: string
  name: string
  description: string
  icon: string // lucide icon name for reference
  examples: Example[]
}

export const EXAMPLE_CATEGORIES: ExampleCategory[] = [
  {
    id: "getting-started",
    name: "Getting Started",
    description: "Basic operations: inserting facts, querying, and managing knowledge graphs",
    icon: "Rocket",
    examples: [
      {
        name: "Insert & Query",
        description: "Insert tuples into a relation and query them",
        code: `+edge[(1, 2), (2, 3), (3, 4)]
?edge(X, Y)`,
        difficulty: "beginner",
      },
      {
        name: "Multiple Relations",
        description: "Work with multiple relations at once",
        code: `+employee[(1, "Alice", 10), (2, "Bob", 20)]
+department[(10, "Engineering"), (20, "Sales")]
?employee(Id, Name, Dept)
?department(Id, Name)`,
        difficulty: "beginner",
      },
      {
        name: "Delete Facts",
        description: "Remove specific tuples from a relation",
        code: `+edge[(1, 2), (2, 3), (3, 4), (4, 5)]
?edge(X, Y)
-edge[(2, 3)]
?edge(X, Y)`,
        difficulty: "beginner",
      },
      {
        name: "Bulk Insert",
        description: "Insert many tuples at once using batch syntax",
        code: `+score[(1, 100), (2, 250), (3, 175), (4, 300), (5, 125), (6, 200)]
?score(Player, Points)`,
        difficulty: "beginner",
      },
      {
        name: "List Relations",
        description: "See what relations exist in the current knowledge graph",
        code: `+edge[(1, 2), (2, 3)]
+node[(1,), (2,), (3,)]
.rel`,
        difficulty: "beginner",
      },
    ],
  },
  {
    id: "schemas",
    name: "Schemas & Types",
    description: "Declare typed schemas with named columns",
    icon: "FileType",
    examples: [
      {
        name: "Explicit Schema",
        description: "Declare relations with typed, named columns",
        code: `+employee(emp_id: int, name: string, dept_id: int)
+department(dept_id: int, dept_name: string)
+employee[(1, "Alice", 10), (2, "Bob", 10), (3, "Charlie", 20)]
+department[(10, "Engineering"), (20, "Sales")]
?employee(Id, Name, Dept)`,
        difficulty: "beginner",
      },
      {
        name: "Schema with Views",
        description: "Use schema columns in rule definitions",
        code: `+employee(emp_id: int, name: string, dept_id: int)
+department(dept_id: int, dept_name: string)
+employee[(1, "Alice", 10), (2, "Bob", 20)]
+department[(10, "Engineering"), (20, "Sales")]
+emp_dept(EmpName, DeptName) <-
  employee(_, EmpName, DeptId),
  department(DeptId, DeptName)
?emp_dept(E, D)`,
        difficulty: "intermediate",
      },
    ],
  },
  {
    id: "rules",
    name: "Rules & Views",
    description: "Define persistent and session rules to create derived views",
    icon: "GitBranch",
    examples: [
      {
        name: "Simple Rule",
        description: "Define a persistent view that joins two relations",
        code: `+employee[(1, 100), (2, 100), (3, 200)]
+department[(100, 10), (200, 20)]
+emp_dept(EmpId, DeptName) <- employee(EmpId, DeptId), department(DeptId, DeptName)
?emp_dept(X, Y)`,
        difficulty: "beginner",
      },
      {
        name: "Session Rule",
        description: "Temporary rules that only live in the current session",
        code: `+edge[(1, 2), (2, 3), (3, 4)]
// Session rules (no + prefix) are ephemeral
path(X, Y) <- edge(X, Y)
path(X, Z) <- path(X, Y), edge(Y, Z)
?path(1, X)`,
        difficulty: "intermediate",
      },
      {
        name: "Multiple Rules Same Head",
        description: "Union of results from multiple rule clauses",
        code: `+edge[(1, 2), (2, 3)]
+shortcut[(1, 3)]
+reachable(X, Y) <- edge(X, Y)
+reachable(X, Y) <- shortcut(X, Y)
?reachable(X, Y)`,
        difficulty: "beginner",
      },
      {
        name: "Rule with Filter",
        description: "Rules can include comparison conditions in the body",
        code: `+score[(1, 100), (2, 250), (3, 175), (4, 300)]
+high_score(Player, Points) <- score(Player, Points), Points > 200
?high_score(X, Y)`,
        difficulty: "beginner",
      },
    ],
  },
  {
    id: "joins",
    name: "Joins",
    description: "Combine data from multiple relations using shared variables",
    icon: "Merge",
    examples: [
      {
        name: "Two-Way Join",
        description: "Join employees to departments on shared key",
        code: `+employee[(1, 100), (2, 100), (3, 200), (4, 200)]
+department[(100, 10), (200, 20)]
+emp_dept(EmpId, DeptName) <- employee(EmpId, DeptId), department(DeptId, DeptName)
?emp_dept(X, Y)`,
        difficulty: "beginner",
      },
      {
        name: "Self Join",
        description: "Join a relation with itself to find patterns",
        code: `+edge[(1, 2), (2, 3), (3, 4)]
// Find 2-hop paths: X -> _ -> Y
+two_hop(X, Y) <- edge(X, Z), edge(Z, Y)
?two_hop(X, Y)`,
        difficulty: "intermediate",
      },
      {
        name: "Triangle Pattern",
        description: "Find triangles in a graph using a three-way join",
        code: `+edge[(1, 2), (2, 3), (3, 1), (1, 3)]
+triangle(A, B, C) <- edge(A, B), edge(B, C), edge(C, A), A < B, B < C
?triangle(X, Y, Z)`,
        difficulty: "intermediate",
      },
      {
        name: "Multi-Way Join",
        description: "Join across four relations",
        code: `+student[(1, "Alice"), (2, "Bob")]
+enrolled[(1, 101), (2, 102)]
+course[(101, "Math"), (102, "History")]
+grade[(1, 101, 95), (2, 102, 88)]
+transcript(Name, Course, Score) <-
  student(SId, Name), enrolled(SId, CId),
  course(CId, Course), grade(SId, CId, Score)
?transcript(N, C, S)`,
        difficulty: "intermediate",
      },
    ],
  },
  {
    id: "filters",
    name: "Filters & Comparisons",
    description: "Filter results using equality, inequality, and range conditions",
    icon: "Filter",
    examples: [
      {
        name: "Comparison Operators",
        description: "Use <, >, <=, >= to filter numeric data",
        code: `+score[(1, 100), (2, 250), (3, 50), (4, 300), (5, 175)]
+high(Player, Pts) <- score(Player, Pts), Pts >= 200
+low(Player, Pts) <- score(Player, Pts), Pts < 100
?high(X, Y)
?low(X, Y)`,
        difficulty: "beginner",
      },
      {
        name: "Range Filter",
        description: "Filter values within a range",
        code: `+reading[(1, 23.5), (2, 45.2), (3, 31.0), (4, 50.1)]
+in_range(Id, V) <- reading(Id, V), V >= 25.0, V <= 45.0
?in_range(X, Y)`,
        difficulty: "beginner",
      },
      {
        name: "Equality Filter",
        description: "Match specific values using variable binding",
        code: `+employee[(1, "Alice", "Engineering"), (2, "Bob", "Sales"), (3, "Charlie", "Engineering")]
+engineering(Id, Name) <- employee(Id, Name, "Engineering")
?engineering(X, Y)`,
        difficulty: "beginner",
      },
    ],
  },
  {
    id: "negation",
    name: "Negation & Set Operations",
    description: "Set difference, anti-join, and universal quantification using negation",
    icon: "MinusCircle",
    examples: [
      {
        name: "Set Difference",
        description: "Find edges not in a skip list using negation",
        code: `+edge[(1, 2), (2, 3), (3, 4), (4, 5)]
+skip[(2, 3), (4, 5)]
+filtered(X, Y) <- edge(X, Y), !skip(X, Y)
?filtered(X, Y)`,
        difficulty: "intermediate",
      },
      {
        name: "Anti-Join",
        description: "Find items that don't match in another relation",
        code: `+student[(1,), (2,), (3,), (4,)]
+enrolled[(1,), (3,)]
+not_enrolled(S) <- student(S), !enrolled(S)
?not_enrolled(X)`,
        difficulty: "intermediate",
      },
      {
        name: "Universal Quantification",
        description: "Check if all items satisfy a condition (via double negation)",
        code: `+employee[(1, "eng"), (2, "eng"), (3, "sales")]
+all_eng(X) <- employee(X, "eng")
// Find departments where NOT all employees are engineering
+has_non_eng(D) <- employee(_, D), !all_eng(_)
?has_non_eng(X)`,
        difficulty: "advanced",
      },
    ],
  },
  {
    id: "recursion",
    name: "Recursion",
    description: "Transitive closure, path finding, and graph algorithms",
    icon: "Repeat",
    examples: [
      {
        name: "Transitive Closure",
        description: "Find all reachable pairs in a graph",
        code: `+edge[(1, 2), (2, 3), (3, 4), (4, 5)]
+connected(X, Y) <- edge(X, Y)
+connected(X, Z) <- edge(X, Y), connected(Y, Z)
?connected(X, Y)`,
        difficulty: "intermediate",
      },
      {
        name: "Ancestor Relation",
        description: "Compute transitive parent-child ancestry",
        code: `+parent[("Alice", "Bob"), ("Bob", "Charlie"), ("Charlie", "David")]
+ancestor(X, Y) <- parent(X, Y)
+ancestor(X, Z) <- parent(X, Y), ancestor(Y, Z)
?ancestor(X, Y)`,
        difficulty: "intermediate",
      },
      {
        name: "Connected Components",
        description: "Find bidirectional connectivity in an undirected graph",
        code: `+edge[(1, 2), (2, 3), (5, 6)]
+connected(X, Y) <- edge(X, Y)
+connected(X, Y) <- edge(Y, X)
+connected(X, Z) <- connected(X, Y), connected(Y, Z)
?connected(X, Y)`,
        difficulty: "intermediate",
      },
      {
        name: "Shortest Path Length",
        description: "Compute minimum-hop distance between nodes",
        code: `+edge[(1, 2), (2, 3), (3, 4), (1, 3)]
+dist(X, Y, 1) <- edge(X, Y)
+dist(X, Z, min<D>) <- dist(X, Y, D1), edge(Y, Z), D = D1 + 1
?dist(1, X, D)`,
        difficulty: "advanced",
      },
    ],
  },
  {
    id: "aggregations",
    name: "Aggregations",
    description: "COUNT, SUM, MIN, MAX, AVG, TOP-K, and grouped aggregations",
    icon: "Sigma",
    examples: [
      {
        name: "Count per Group",
        description: "Count tuples in each group",
        code: `+sales[(1, 100), (1, 200), (1, 150), (2, 300), (2, 250), (3, 500)]
+sales_count(Region, count<Amount>) <- sales(Region, Amount)
?sales_count(X, Y)`,
        difficulty: "beginner",
      },
      {
        name: "Sum & Average",
        description: "Compute sum and average per group",
        code: `+sales[(1, 100), (1, 200), (2, 300), (2, 250)]
+total(Region, sum<Amount>) <- sales(Region, Amount)
+average(Region, avg<Amount>) <- sales(Region, Amount)
?total(X, Y)
?average(X, Y)`,
        difficulty: "beginner",
      },
      {
        name: "Min & Max",
        description: "Find minimum and maximum values per group",
        code: `+score[("Alice", 85), ("Alice", 92), ("Bob", 78), ("Bob", 95)]
+best(Name, max<Score>) <- score(Name, Score)
+worst(Name, min<Score>) <- score(Name, Score)
?best(X, Y)
?worst(X, Y)`,
        difficulty: "beginner",
      },
      {
        name: "Top-K",
        description: "Get the top N results ordered by a column",
        code: `+score[(1, 100), (2, 250), (3, 175), (4, 300), (5, 125), (6, 200)]
+top_players(top_k<3, Player, Points:desc>) <- score(Player, Points)
?top_players(X, Y)`,
        difficulty: "intermediate",
      },
      {
        name: "Count Distinct",
        description: "Count unique values in a column",
        code: `+purchase[("Alice", "Laptop"), ("Alice", "Phone"), ("Alice", "Laptop"), ("Bob", "Phone")]
+unique_products(Buyer, count_distinct<Product>) <- purchase(Buyer, Product)
?unique_products(X, Y)`,
        difficulty: "intermediate",
      },
    ],
  },
  {
    id: "arithmetic",
    name: "Arithmetic & Math",
    description: "Mathematical operations, computed columns, and math functions",
    icon: "Calculator",
    examples: [
      {
        name: "Computed Columns",
        description: "Create new columns from arithmetic expressions",
        code: `+item[(1, 100, 0.08), (2, 200, 0.10)]
+total(Id, Price, Tax, Final) <- item(Id, Price, Rate), Tax = Price * Rate, Final = Price + Tax
?total(Id, Price, Tax, Final)`,
        difficulty: "beginner",
      },
      {
        name: "Modulo & Division",
        description: "Integer division and remainder operations",
        code: `+number[(10,), (15,), (20,), (25,)]
+result(N, Div, Mod) <- number(N), Div = N / 7, Mod = N % 7
?result(X, Y, Z)`,
        difficulty: "beginner",
      },
      {
        name: "Math Functions",
        description: "Built-in mathematical functions",
        code: `+val[(-5,), (16,), (2.5,)]
+computed(V, A) <- val(V), A = abs_int64(V)
?computed(X, Y)`,
        difficulty: "intermediate",
      },
    ],
  },
  {
    id: "strings",
    name: "String Operations",
    description: "String manipulation functions: concat, length, substring, and more",
    icon: "Type",
    examples: [
      {
        name: "String Concat",
        description: "Concatenate strings together",
        code: `+parts[("hello", " ", "world"), ("foo", "", "bar")]
concat_result(A, B, C, R) <- parts(A, B, C), R = CONCAT(A, B, C)
?concat_result(A, B, C, R)`,
        difficulty: "beginner",
      },
      {
        name: "String Length",
        description: "Compute the length of strings",
        code: `+words[("hello",), ("world",), ("hi",)]
+lengths(W, L) <- words(W), L = LEN(W)
?lengths(X, Y)`,
        difficulty: "beginner",
      },
      {
        name: "String Contains",
        description: "Check if a string contains a substring",
        code: `+text[("hello world",), ("foo bar",), ("hello there",)]
has_hello(T) <- text(T), CONTAINS(T, "hello") = true
?has_hello(X)`,
        difficulty: "intermediate",
      },
    ],
  },
  {
    id: "vectors",
    name: "Vector Search",
    description: "Vector operations: distance calculations, similarity search",
    icon: "Compass",
    examples: [
      {
        name: "Euclidean Distance",
        description: "Calculate distances between vectors",
        code: `+vectors[(1, [1.0, 0.0, 0.0]), (2, [0.0, 1.0, 0.0]), (3, [1.0, 1.0, 0.0])]
+query[([0.0, 0.0, 0.0])]
nearest(Id, Dist) <- vectors(Id, V), query(Q), Dist = euclidean(V, Q)
?nearest(X, Y)`,
        difficulty: "intermediate",
      },
      {
        name: "Cosine Similarity",
        description: "Find similar vectors using cosine distance",
        code: `+docs[(1, [1.0, 0.0]), (2, [0.7, 0.7]), (3, [0.0, 1.0])]
+query[([1.0, 0.0])]
sim(Id, D) <- docs(Id, V), query(Q), D = cosine(V, Q)
?sim(X, Y)`,
        difficulty: "intermediate",
      },
    ],
  },
  {
    id: "temporal",
    name: "Temporal Operations",
    description: "Working with timestamps, time differences, and time-based queries",
    icon: "Clock",
    examples: [
      {
        name: "Current Timestamp",
        description: "Use time_now() to get the current Unix timestamp in milliseconds",
        code: `+ref[(1, 1736640000000)]
valid_ref(Id, RefTime) <- ref(Id, RefTime), Now = time_now(), Now > RefTime
?valid_ref(Id, RefTime)`,
        difficulty: "intermediate",
      },
    ],
  },
  {
    id: "mutations",
    name: "Data Mutations",
    description: "Conditional deletes, updates, and atomic operations",
    icon: "Pencil",
    examples: [
      {
        name: "Conditional Delete",
        description: "Delete facts that match a condition",
        code: `+counter[(1, 100), (2, 200), (3, 50), (4, 150)]
?counter(Id, Value)
// Delete where value > 100
-counter(Id, Value) <- counter(Id, Value), Value > 100
?counter(Id, Value)`,
        difficulty: "intermediate",
      },
      {
        name: "Update Pattern",
        description: "Simulate updates via delete-then-insert",
        code: `+counter[(1, 100), (2, 200), (3, 50)]
?counter(Id, Value)
// Delete old value, insert new value
-counter[(2, 200)]
+counter[(2, 210)]
?counter(Id, Value)`,
        difficulty: "intermediate",
      },
      {
        name: "Wildcard Delete",
        description: "Delete all tuples matching a partial pattern",
        code: `+edge[(1, 2), (1, 3), (1, 4), (2, 3)]
?edge(X, Y)
// Delete all edges from node 1
-edge(1, Y) <- edge(1, Y)
?edge(X, Y)`,
        difficulty: "intermediate",
      },
    ],
  },
  {
    id: "sessions",
    name: "Sessions",
    description: "Ephemeral session rules and facts that don't persist",
    icon: "Timer",
    examples: [
      {
        name: "Session Facts",
        description: "Add temporary facts that only exist in the current session",
        code: `+person[(1,), (2,), (3,), (4,)]
// Session facts (no + prefix)
blocked(2)
blocked(4)
allowed(X) <- person(X), !blocked(X)
?allowed(X)`,
        difficulty: "intermediate",
      },
      {
        name: "Session Rules",
        description: "Define temporary rules without persisting them",
        code: `+edge[(1, 2), (2, 3), (3, 4)]
// Session rules (no + prefix)
path(X, Y) <- edge(X, Y)
path(X, Z) <- path(X, Y), edge(Y, Z)
?path(1, X)
// View session state
.session`,
        difficulty: "intermediate",
      },
    ],
  },
  {
    id: "meta",
    name: "Meta Commands",
    description: "System commands for managing knowledge graphs, rules, and relations",
    icon: "Terminal",
    examples: [
      {
        name: "Knowledge Graph Management",
        description: "Create, switch, list, and drop knowledge graphs",
        code: `.kg create my_graph
.kg use my_graph
+node[(1,), (2,), (3,)]
.kg
.kg use default
.kg drop my_graph`,
        difficulty: "beginner",
      },
      {
        name: "Inspect Relations",
        description: "List relations, view details, and check schema",
        code: `+edge[(1, 2), (2, 3)]
+node[(1,), (2,), (3,)]
.rel
.rel edge`,
        difficulty: "beginner",
      },
      {
        name: "Rule Management",
        description: "Add, list, and remove rule clauses",
        code: `+edge[(1, 2), (2, 3), (3, 4)]
+path(X, Y) <- edge(X, Y)
+path(X, Z) <- edge(X, Y), path(Y, Z)
.rule
.rule path
// Remove first clause
.rule remove path 1
.rule path`,
        difficulty: "intermediate",
      },
    ],
  },
  {
    id: "applications",
    name: "Applications",
    description: "Real-world patterns: RBAC, social networks, bill of materials",
    icon: "Boxes",
    examples: [
      {
        name: "RBAC Permissions",
        description: "Role-based access control with permission inheritance",
        code: `// Role hierarchy: admin(1) > manager(2) > employee(3)
+role_inherits[(1, 2), (2, 3)]
+user_role[(100, 1), (101, 2), (102, 3)]
+role_permission[(1, 1000), (2, 1002), (3, 1004)]
// Inherited roles (transitive)
+has_role(User, Role) <- user_role(User, Role)
+has_role(User, Parent) <- has_role(User, Child), role_inherits(Child, Parent)
// Effective permissions
+has_permission(User, Perm) <- has_role(User, Role), role_permission(Role, Perm)
?has_permission(User, Perm)`,
        difficulty: "advanced",
      },
      {
        name: "Friends of Friends",
        description: "Social network graph traversal",
        code: `+friends[("Alice", "Bob"), ("Bob", "Charlie"), ("Charlie", "David"), ("Alice", "Eve")]
// Symmetric friendship
+knows(X, Y) <- friends(X, Y)
+knows(X, Y) <- friends(Y, X)
// Friends of friends
+fof(X, Z) <- knows(X, Y), knows(Y, Z), X != Z
?fof("Alice", X)`,
        difficulty: "intermediate",
      },
      {
        name: "Bill of Materials",
        description: "Component explosion: find all parts of an assembly",
        code: `// part_of(component, assembly)
+part_of[("wheel", "car"), ("engine", "car"), ("piston", "engine"), ("spark_plug", "engine")]
+all_parts(Part, Assembly) <- part_of(Part, Assembly)
+all_parts(Part, Top) <- part_of(Part, Sub), all_parts(Sub, Top)
?all_parts(Part, "car")`,
        difficulty: "advanced",
      },
      {
        name: "Org Chart Levels",
        description: "Compute management levels in an organization hierarchy",
        code: `+reports_to[("Bob", "Alice"), ("Charlie", "Alice"), ("David", "Bob"), ("Eve", "Bob")]
+level("Alice", 0)
+level(Emp, L) <- reports_to(Emp, Mgr), level(Mgr, ML), L = ML + 1
?level(X, Y)`,
        difficulty: "advanced",
      },
    ],
  },
]

/** Flatten all examples for search */
export function getAllExamples(): (Example & { categoryId: string; categoryName: string })[] {
  return EXAMPLE_CATEGORIES.flatMap((cat) =>
    cat.examples.map((ex) => ({ ...ex, categoryId: cat.id, categoryName: cat.name })),
  )
}

/** Get total example count */
export function getExampleCount(): number {
  return EXAMPLE_CATEGORIES.reduce((sum, cat) => sum + cat.examples.length, 0)
}
