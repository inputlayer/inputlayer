# Aggregations Reference

InputLayer supports 9 aggregation functions for computing summary values over groups of data.

## Basic Aggregations

### `count`

Count the number of results.

**Syntax:**
```datalog
count<Variable>
```

**Example:**
```datalog
// Count all users
+user_count(count<Id>) <- user(Id, _)

// Count users per department
+dept_count(Dept, count<Id>) <- user(Id, _, Dept)
```

**Returns:** Integer count of distinct bindings for the variable.

---

### `count_distinct`

Count distinct values of a variable.

**Syntax:**
```datalog
count_distinct<Variable>
```

**Example:**
```datalog
// Count unique departments
+unique_depts(count_distinct<Dept>) <- user(_, _, Dept)
```

**Returns:** Integer count of unique values.

---

### `sum`

Sum numeric values.

**Syntax:**
```datalog
sum<Variable>
```

**Example:**
```datalog
// Total salary per department
+dept_salary(Dept, sum<Salary>) <- employee(_, _, Dept, Salary)
```

**Returns:** Sum as Integer or Float (matches input type).

---

### `min`

Find minimum value.

**Syntax:**
```datalog
min<Variable>
```

**Example:**
```datalog
// Lowest price per category
+min_price(Category, min<Price>) <- product(_, Category, Price)
```

**Returns:** Minimum value (works with numbers and strings).

---

### `max`

Find maximum value.

**Syntax:**
```datalog
max<Variable>
```

**Example:**
```datalog
// Highest score per game
+high_score(Game, max<Score>) <- scores(_, Game, Score)
```

**Returns:** Maximum value (works with numbers and strings).

---

### `avg`

Compute average of numeric values.

**Syntax:**
```datalog
avg<Variable>
```

**Example:**
```datalog
// Average rating per product
+avg_rating(Product, avg<Rating>) <- reviews(_, Product, Rating)
```

**Returns:** Float average.

---

## Ranking Aggregations

### `top_k`

Select the top K results ordered by a variable.

**Syntax:**
```datalog
top_k<K, PassThrough..., OrderVariable>        // Ascending order (lowest K)
top_k<K, PassThrough..., OrderVariable:desc>   // Descending order (highest K)
```

**Parameters:**
- `K` - Number of results to return (integer)
- `PassThrough.` - Variables to include in result (optional)
- `OrderVariable` - Variable to order by
- `desc` - Optional suffix on OrderVariable for descending order

**Example:**
```datalog
// Top 10 highest scores
+top_scores(top_k<10, Name, Score:desc>) <- scores(Name, Score)

// Top 5 nearest neighbors by distance
+nearest(top_k<5, Id, Dist>) <-
    query_vec(QV),
    vectors(Id, V),
    Dist = euclidean(QV, V)
```

**Returns:** Up to K results with the ordering value.

---

### `top_k_threshold`

Select top K results, but only if they meet a minimum threshold.

**Syntax:**
```datalog
top_k_threshold<K, Threshold, PassThrough..., OrderVariable>        // Ascending
top_k_threshold<K, Threshold, PassThrough..., OrderVariable:desc>   // Descending
```

**Parameters:**
- `K` - Maximum number of results
- `Threshold` - Minimum (or maximum for desc) value to include
- `PassThrough.` - Variables to include in result (optional)
- `OrderVariable` - Variable to order by
- `desc` - Optional suffix on OrderVariable for descending order

**Example:**
```datalog
// Top 10 products, but only if rating >= 4.0
+top_rated(top_k_threshold<10, 4.0, Product, Rating:desc>) <-
    reviews(Product, Rating)

// Nearest 5 neighbors within distance 0.5
+near_enough(top_k_threshold<5, 0.5, Id, Dist>) <-
    query_vec(QV),
    vectors(Id, V),
    Dist = euclidean(QV, V)
```

**Returns:** Up to K results that meet the threshold.

---

### `within_radius`

Return all results within a distance threshold (range query).

**Syntax:**
```datalog
within_radius<MaxDistance, PassThrough..., DistanceVariable>
```

**Parameters:**
- `MaxDistance` - Maximum distance to include
- `PassThrough.` - Variables to include in result (optional)
- `DistanceVariable` - Variable containing the distance

**Example:**
```datalog
// All vectors within distance 0.3
+nearby(within_radius<0.3, Id, Dist>) <-
    query_vec(QV),
    vectors(Id, V),
    Dist = cosine(QV, V)

// Points within 100 meters
+close_points(within_radius<100.0, Id, D>) <-
    my_location(Lat1, Lon1),
    locations(Id, Lat2, Lon2),
    D = haversine(Lat1, Lon1, Lat2, Lon2)
```

**Returns:** All results where distance ≤ MaxDistance.

---

## Aggregation Rules

### Grouping

Variables in the head that are NOT aggregated become grouping keys:

```datalog
// Group by Department, aggregate Salary
+dept_stats(Dept, sum<Salary>, avg<Salary>) <-
    employee(_, _, Dept, Salary)
```

### Multiple Aggregations

Multiple aggregations can be combined in one rule:

```datalog
+stats(count<Id>, min<Value>, max<Value>, avg<Value>) <-
    data(Id, Value)
```

### With Filters

Aggregations work with filter conditions:

```datalog
// Only count active users
+active_count(count<Id>) <- user(Id, Active), Active = true

// Sum only positive values
+positive_sum(sum<V>) <- values(V), V > 0
```

---

## Quick Reference

| Function | Syntax | Returns |
|----------|--------|---------|
| `count` | `count<X>` | Integer count |
| `count_distinct` | `count_distinct<X>` | Integer unique count |
| `sum` | `sum<X>` | Sum (Int or Float) |
| `min` | `min<X>` | Minimum value |
| `max` | `max<X>` | Maximum value |
| `avg` | `avg<X>` | Float average |
| `top_k` | `top_k<K, ..., X>` or `top_k<K, ..., X:desc>` | Top K results |
| `top_k_threshold` | `top_k_threshold<K, T, ..., X>` | Top K meeting threshold |
| `within_radius` | `within_radius<Max, ..., D>` | All within distance |
