# Databricks notebook source
# MAGIC %md
# MAGIC # DELTA TABLE SCHEMA COMPARATOR
# MAGIC ### Put your two delta table below to compare them

# COMMAND ----------

#Add your configuration parameters for external delta table using Python or Scala

# COMMAND ----------

#ADD FIRST TABLE
table1 = ""

# COMMAND ----------

#ADD SECOND TABLE
table2 = ""

# COMMAND ----------

missing_fields = []
type_mismatches = [] 

df1 = spark.read.format("delta").table(table1)
df2 = spark.read.format("delta").table(table2)

schema1 = df1.schema
schema2 = df2.schema

for field1 in schema1:
    for field2 in schema2:
        if field1.name == field2.name:
            if field1.dataType != field2.dataType:
              str_type = f"Column '{field1.name}' has a type mismatch: {field1.dataType} vs {field2.dataType}"  
              type_mismatches.append(str_type)

column_names_1 = [field.name for field in schema1.fields]
column_names_2 = [field.name for field in schema2.fields]

difference_1 = list(set(column_names_1) - set(column_names_2))
str_diff = f"Elements in table1 but not in table2: {', '.join(map(str, difference_1))}"
missing_fields.append(str_diff)
difference_2 = list(set(column_names_2) - set(column_names_1))
str_diff = f"Elements in table2 but not in table1: {', '.join(map(str, difference_2))}"
missing_fields.append(str_diff)

# COMMAND ----------

print("Missing Keys:")
for diff in missing_fields:
    print(diff)
    print("\n")

# COMMAND ----------

print("Type Mismatches:")
for diff in type_mismatches:
    print(diff)