# Spark 3.4 Release Summary Features
> Credits :  Databricks
---
## Spark SQL Features
- 40 new functions added

### 1. Default Values for columns in Tables
```SQL
-- creation with default
CREATE TABLE T1(
    id INT,
    current_date DATE DEFAULT CURRENT_DATE()
)
USING PARQUET;
```
```SQL
-- alter tables with default
ALTER TABLE T2 
ALTER COLUMN this_date
    SET DEFAULT CURRENT_DATE();
```
```SQL
-- use the default values in insert
INSERT INTO T1 VALUES 
(0,DEFAULT),
(1,DEFAULT),
(2,DATE '2020-11-30');
```
> For any column with no explicit `DEFAULT`  will be assigned NULL if `DEFAULT` is used to insert value for it.
```SQL
CREATE TABLE T1(
    id INT,
    current_date DATE DEFAULT CURRENT_DATE()
)
USING PARQUET;

INSERT INTO T1 VALUES 
(0,DEFAULT),
(DEFAULT,DEFAULT),
(2,DATE '2020-11-30');
```
This insert returns NULL for 2nd row as id doesn't has any defaults in creation
|id|current_date|
|-|-|
|0|2024-11-30|
|NULL|2024-11-30|
|2|2020-11-30|

> Use `DESCRIBE EXTENDED` command to check for `DEFAULT`s

If the table has 3 cols with 2 columns with `DEFAULT`s  then at **INSERT** you can only specify the column that does not has a default. Spark will automatically put the `DEFAULT` values in the other 2 cols
```SQL
CREATE TABLE T4(
    id INT, -- only col without defaults
    current_date DATE DEFAULT CURRENT_DATE(),
    value INT DEFAULT 1000
)
USING PARQUET;

-- Insert for only non default values
INSERT INTO T4 VALUES 
(0),
(DEFAULT), -- No defaults mentioned so this will return NULL
(2);
```
This results : 
|id|current_date|value|
|-|-|-|
|0|2024-11-30|1000|
|NULL|2024-11-30|1000|
|2|2024-11-30|1000|

`DEFAULT` also works for **UPDATE** & **MERGE**
```SQL
UPDATE T3 
SET current_date = DEFAULT
WHERE id = 2 ;
```
```SQL
MERGE INTO t FROM VALUES(42,DATE '2024-11-30')
AS S(c1,c2)
USING  first  = c1
WHEN NOT MATCHED THEN
    INSERT(first,second) = (c1, DEFAULT)
WHEN MATCHED THEN
    UPDATE SET (second = DEFAULT)
```

### 2. Added new datatype -  Timestamp without timezone (TIMESTAMP_NTZ)

|Data Type|Comment|
|-|-|
|`TIMESTAMP`|Timestamp at Local TimeZone of the session|
|`TIMESTAMP_NTZ`|-|

```SQL
CREATE TABLE tz(
    timestamp_without_timezone TIMESTAMP_NTZ
)
using parquet;

INSERT INTO tz VALUES
(TIMESTAMP_NTZ'2024-11-30 10:11:12.123456');
```
converting between `TIMEZONE` from `TIMESTAMP_NTZ` :

Function : `convert_timezone([sourceTz, ]targetTz, sourceTs)`
> `sourceTz` : If absent, the current session time zone is used as the source time zone
```SQL
SELECT convert_timezone('America/Los_Angeles', 'UTC', timestamp_ntz'2021-12-06 00:00:00');

-- returns : 2021-12-06 08:00:00
```
```SQL
SET TIME ZONE 'America/Los_Angeles';
SELECT convert_timezone( 'UTC', timestamp_ntz'2021-12-06 00:00:00');

-- returns : 2021-12-06 08:00:00
```
### 3. Lateral Column Alias References :

lateral column ref can be used in `SELECT` clause  to refer previous items
- simplifies queries
- you can replace lot of sub queries  & CTEs in some cases.

```SQL
CREATE TABLE t1 (
    salary INT,
    bonus INT,
    name STRING
) using parquet;

INSERT INTO t1  VALUES (1000,100, 'john');
INSERT INTO t1  VALUES (2000,200, 'doe');
```

**Without Alias Ref** :
```SQL
-- Without Alias
WITH w AS (
    SELECT 
        *,
        (salary * 2)  AS new_salary 
    FROM t1
)
SELECT 
    new_salary,
    (new_salary+bonus) AS new_salary_with_bonus
FROM w
WHERE name = 'john';
```

**spark 3.4 Alias Ref** :
```SQL
-- With Alias
SELECT 
    salary*2 AS new_salary,
    new_salary+bonus AS new_salary_with_bonus -- the prev col 'new_salary' can be referred here without CTE
FROM t1
WHERE name = 'john';
```

### 4. Dataset.to(StructType) :

Table insert matches the inut query to adjust the input schema. In case of complex struct fields it was not possible earlier. With this new change it is possible.

```python
from pyspark.sql.functions import struct
from pyspark.sql.types import StructType, StringType

data = [("a", "b")]
df = spark.createDataFrame(data, ["i", "j"])

inner_fields = StructType().add("J", StringType()).add("I", StringType())
schema = StructType().add("struct", inner_fields, False)

df = df.select(struct(df["i"], df["j"]).alias("struct")).to(schema) # `.to(schema)`  updates this schema

assert df.schema == schema # matches 

result = df.collect()
print(result)

# returns (b,a)
```
### 5. Parameterize SQL Queries (Normal, Named  & Unnamed) : 

> `.to(schema)` opens up for SQL Injections.  To handle this  Parameterize SQL Queries are introduced.

**Normal :**
```python
# simple example 
spark.sql(
    sqlText = """
        SELECT 
            id1, 
            SUM(v1) AS v1 
        FROM h20_1e9 
        WHERE id1 = {id1_val} 
        GROUP BY id1
    """,
    id1_val="id016"
).show()

```
**named-parameter-markers :**
> syntax `:parameter_name` 
```python

spark.sql(
    sqlText = """
        SELECT 
        item, 
        sum(amount) 
        FROM purchase 
        GROUP BY item 
        HAVING item = :item
    """,
    args={"item": "shirt"}
).show()
```

```python
spark.sql(
    "SELECT :x * :y * :z AS volume", 
    args = { 
        "x" : 3, 
        "y" : 4, 
        "z"  : 5 
    }
    ).show()
```
|volume|
|-|
|60|

**Unnamed-parameter-markers :**
> syntax `:?` 
```python
spark.sql(
    "SELECT ? * ? * ? AS volume", 
    args = { 3, 4, 5 }
).show()
```
|volume|
|-|
|60|


