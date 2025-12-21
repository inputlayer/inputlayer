# Tutorial: Basic Queries

This tutorial covers the fundamentals of querying data in InputLayer.

## Prerequisites

- Completed [Your First Program](../getting-started/02-first-program.md)
- Understanding of [Core Concepts](../getting-started/03-core-concepts.md)

## Setup

Let's create a sample database to work with:

```datalog
.db create query_tutorial
.db use query_tutorial

// People: (id, name, age, city)
+person[(1, "alice", 30, "NYC"),
        (2, "bob", 25, "LA"),
        (3, "carol", 35, "NYC"),
        (4, "dave", 28, "Chicago"),
        (5, "eve", 32, "LA")].

// Friendships: (person1_id, person2_id)
+friends[(1, 2), (1, 3), (2, 4), (3, 5), (4, 5)].

// Purchases: (person_id, item, amount)
+purchase[(1, "laptop", 1200),
          (1, "phone", 800),
          (2, "tablet", 500),
          (3, "laptop", 1100),
          (4, "phone", 900),
          (5, "tablet", 450)].
```

## Basic Pattern Matching

### Query All Data

```datalog
?- person(Id, Name, Age, City).
```

Result:
```
Results: 5 rows
  (1, "alice", 30, "NYC")
  (2, "bob", 25, "LA")
  (3, "carol", 35, "NYC")
  (4, "dave", 28, "Chicago")
  (5, "eve", 32, "LA")
```

### Query with Constants

Find a specific person by ID:

```datalog
?- person(1, Name, Age, City).
```

Result:
```
Results: 1 row
  (1, "alice", 30, "NYC")
```

Find all people in NYC:

```datalog
?- person(Id, Name, Age, "NYC").
```

Result:
```
Results: 2 rows
  (1, "alice", 30, "NYC")
  (3, "carol", 35, "NYC")
```

### Ignoring Columns with `_`

Get just names and ages (ignore id and city):

```datalog
?- person(_, Name, Age, _).
```

Get just unique cities:

```datalog
?- person(_, _, _, City).
```

## Filtering with Constraints

### Comparison Operators

Find people over 30:

```datalog
?- person(Id, Name, Age, City), Age > 30.
```

Result:
```
Results: 2 rows
  (3, "carol", 35, "NYC")
  (5, "eve", 32, "LA")
```

Available operators: `=`, `!=`, `<`, `<=`, `>`, `>=`

### Multiple Constraints

People in NYC who are over 30:

```datalog
?- person(Id, Name, Age, "NYC"), Age > 30.
```

Result:
```
Results: 1 row
  (3, "carol", 35, "NYC")
```

People aged 25-30:

```datalog
?- person(Id, Name, Age, City), Age >= 25, Age <= 30.
```

## Joins

### Two-Way Joins

Find friends of alice (id=1):

```datalog
?- friends(1, FriendId), person(FriendId, Name, _, _).
```

Result:
```
Results: 2 rows
  (2, "bob")
  (3, "carol")
```

**How it works:** The variable `FriendId` appears in both atoms, creating a join.

### Self-Joins

Find mutual friendships (both directions exist):

```datalog
?- friends(A, B), friends(B, A).
```

Find friends of friends:

```datalog
?- friends(1, Mid), friends(Mid, FoF).
```

### Multi-Way Joins

Find the city of alice's friends:

```datalog
?- person(1, "alice", _, _),    // Start with alice
   friends(1, FId),              // Get her friend IDs
   person(FId, FName, _, FCity). // Get friend details
```

Result:
```
Results: 2 rows
  ("bob", "LA")
  ("carol", "NYC")
```

## Creating Derived Relations

### Simple Rules

Create a rule for NYC residents:

```datalog
+nyc_resident(Id, Name, Age) :- person(Id, Name, Age, "NYC").
```

Now query it:

```datalog
?- nyc_resident(Id, Name, Age).
```

### Rules with Joins

Create a rule for purchase details with buyer names:

```datalog
+purchase_detail(Name, Item, Amount) :-
  purchase(PersonId, Item, Amount),
  person(PersonId, Name, _, _).
```

Query:
```datalog
?- purchase_detail(Name, Item, Amount).
```

Result:
```
Results: 6 rows
  ("alice", "laptop", 1200)
  ("alice", "phone", 800)
  ("bob", "tablet", 500)
  ("carol", "laptop", 1100)
  ("dave", "phone", 900)
  ("eve", "tablet", 450)
```

### Rules with Filters

High-value purchases over $1000:

```datalog
+big_spender(Name, Item, Amount) :-
  purchase_detail(Name, Item, Amount),
  Amount > 1000.
```

## Aggregations

### Count

Count people per city:

```datalog
+city_population(City, count<Id>) :-
  person(Id, _, _, City).
```

Query:
```datalog
?- city_population(City, Count).
```

Result:
```
Results: 3 rows
  ("NYC", 2)
  ("LA", 2)
  ("Chicago", 1)
```

### Sum

Total spending per person:

```datalog
+total_spent(Name, sum<Amount>) :-
  purchase_detail(Name, _, Amount).
```

### Min/Max

Find the youngest person in each city:

```datalog
+youngest_in_city(City, min<Age>) :-
  person(_, _, Age, City).
```

Find the most expensive purchase:

```datalog
+max_purchase(max<Amount>) :- purchase(_, _, Amount).
```

### Average

Average age by city:

```datalog
+avg_age_by_city(City, avg<Age>) :-
  person(_, _, Age, City).
```

## Negation

### Basic Negation

People who haven't made any purchase:

```datalog
+non_buyer(Id, Name) :-
  person(Id, Name, _, _),
  !purchase(Id, _, _).
```

### Set Difference

People in NYC but not in the friends table:

```datalog
+lonely_nyc(Id, Name) :-
  person(Id, Name, _, "NYC"),
  !friends(Id, _),
  !friends(_, Id).
```

## Practical Examples

### Example 1: Friend Recommendations

Find friends-of-friends who aren't already friends:

```datalog
// Friends of friends
+fof(Person, FoF) :-
  friends(Person, Friend),
  friends(Friend, FoF),
  Person != FoF.

// Potential new friends (FoF but not already friends)
+recommendation(Person, FoF) :-
  fof(Person, FoF),
  !friends(Person, FoF).
```

### Example 2: Customer Analysis

High-value customers (spent more than $1500 total):

```datalog
+high_value(Name, Total) :-
  total_spent(Name, Total),
  Total > 1500.
```

### Example 3: Geographic Analysis

Cities with above-average age:

```datalog
// First, compute overall average
+overall_avg(avg<Age>) :- person(_, _, Age, _).

// Then find cities above average
+older_city(City, CityAvg) :-
  avg_age_by_city(City, CityAvg),
  overall_avg(OverallAvg),
  CityAvg > OverallAvg.
```

## Query Patterns Summary

| Pattern | Example | Use Case |
|---------|---------|----------|
| Select all | `?- rel(X, Y).` | Get all data |
| Filter by constant | `?- rel(1, Y).` | Find specific records |
| Filter by constraint | `?- rel(X, Y), X > 10.` | Conditional queries |
| Join | `?- r1(X, Y), r2(Y, Z).` | Combine relations |
| Aggregate | `count<X>, sum<Y>` | Summarize data |
| Negation | `!rel(X, _)` | Exclude matches |

## Exercises

1. Find all purchases by people in LA
2. Find the person with the highest total spending
3. Find cities where everyone is over 25
4. Find pairs of people in the same city who are not friends

## Next Steps

- **[Recursion Tutorial](02-recursion.md)** - Recursive queries and graph traversal
- **[Aggregations Tutorial](04-aggregations.md)** - Advanced aggregation patterns
- **[Cheatsheet](../CHEATSHEET.md)** - Quick reference
