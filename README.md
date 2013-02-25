# clojure-data

Clojure-data is a simple, straightforward library for handling SQL, database connections, and data typing in Clojure.

## SQL generation

Templating SQL queries while avoiding SQL injection shouldn't be difficult.  Below is the syntax for a moderately complex SQL statement.

    SELECT (p.first_name || ' ' || p.last_name) as name, p.age
    FROM people as p
    WHERE p.age > 30 AND NOT EXISTS (SELECT 1 FROM ?)

The same query in clojure-data's SQL DSL.

    (select [(as (|| :p.first_name " " :p.last_name)) :p.age]
      (from (as :people :p))
      (where (> :p.age 30)))
