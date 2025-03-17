# Databricks notebook source
# MAGIC %md
# MAGIC # JSON SCHEMA COMPARATOR
# MAGIC ### Put your two json schemas like string

# COMMAND ----------

#ADD FIRST SCHEMA
schema1_str = '''{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "object",
  "properties": {
    "user_id": {
      "type": "integer"
    },
    "first_name": {
      "type": "string"
    },
    "last_name": {
      "type": "string"
    },
    "email": {
      "type": "string",
      "format": "email"
    },
    "date_of_birth": {
      "type": "string",
      "format": "date"
    },
    "is_active": {
      "type": "boolean"
    }
  },
  "additionalProperties": false
}'''

# COMMAND ----------

#ADD SECOND SCHEMA
schema2_str = '''{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "object",
  "properties": {
    "user_id": {
      "type": "integer"
    },
    "first_name": {
      "type": "string"
    },
    "email": {
      "type": "string",
      "format": "email"
    },
    "date_of_birth": {
      "type": "string"
    },
    "is_active": {
      "type": "boolean"
    }
  },
  "additionalProperties": false
}'''

# COMMAND ----------

import json
schema1 = json.loads(schema1_str)
schema2 = json.loads(schema2_str)

# COMMAND ----------

# Lists to track the differences
missing_keys = []  # For missing keys in either schema
type_mismatches = []  # For type mismatches
format_errors = []  # For errors related to .format and other special attributes
additional_properties_errors = [] 

def compare_json_schemas(schema1, schema2, path=None):
    """Recursively compare two JSON schemas and track missing keys, type mismatches, and format errors."""
    if path is None:
        path = []

    # Check for type mismatch at the top level
    if 'type' in schema1 and 'type' in schema2:
        if schema1['type'] != schema2['type']:
            type_mismatches.append(f"Type mismatch at {'.'.join(map(str, path + ['type']))}: {schema1['type']} != {schema2['type']}")

    if isinstance(schema1, dict) and isinstance(schema2, dict):
        # Compare schema properties
        keys_to_jump = ["format", "additionalProperties"]
        for key in schema1:
            if key in keys_to_jump:
                continue
            if key in schema2:
                new_path = path + [key]
                compare_json_schemas(schema1[key], schema2[key], new_path)
            else:
                missing_keys.append(f"Key {'.'.join(map(str, path + [key]))} exists in the first schema but not in the second.")

        # Check for extra keys in the second schema
        for key in schema2:
            if key in keys_to_jump:
                continue
            if key not in schema1:
                missing_keys.append(f"Key {'.'.join(map(str, path + [key]))} exists in the second schema but not in the first.")

    elif isinstance(schema1, list) and isinstance(schema2, list):
        # Compare list items (for example, 'required' fields)
        for idx, (item1, item2) in enumerate(zip(schema1, schema2)):
            new_path = path + [idx]
            compare_json_schemas(item1, item2, new_path)

        # Check if lists are of different lengths
        if len(schema1) != len(schema2):
            missing_keys.append(f"List at {'.'.join(map(str, path))} has different lengths: {len(schema1)} vs {len(schema2)}")

    else:
        # Compare primitive values (strings, integers, etc.)
        if schema1 != schema2:
            type_mismatches.append(f"Difference at {'.'.join(map(str, path))}: {schema1} != {schema2}")
    
    # Check for .format errors in specific properties
    if 'format' in schema1 and 'format' in schema2:
        if schema1['format'] != schema2['format']:
            format_errors.append(f"Format mismatch at {'.'.join(map(str, path))}: {schema1['format']} != {schema2['format']}")
    if 'format' in schema1 and 'format' not in schema2:
        format_errors.append(f"Key {'.'.join(map(str, path))} exists in the first schema but not the second (missing .format).")

    if 'format' in schema2 and 'format' not in schema1:
        format_errors.append(f"Key {'.'.join(map(str, path))} exists in the second schema but not the first (missing .format).")

    #Check AdditionaProperties    
    if 'additionalProperties' in schema1 and 'additionalProperties' in schema2:
        if schema1['additionalProperties'] != schema2['additionalProperties']:
            additional_properties_errors.append(f"additionalProperties  mismatch at {'.'.join(map(str, path))}: {schema1['additionalProperties']} != {schema2['additionalProperties']}")
    if 'additionalProperties' in schema1 and 'additionalProperties' not in schema2:
        additional_properties_errors.append(f"Key {'.'.join(map(str, path))} exists in the first schema but not the second (missing .additionalProperties ).")

    if 'additionalProperties' in schema2 and 'additionalProperties' not in schema1:
        additional_properties_errors.append(f"Key {'.'.join(map(str, path))} exists in the second schema but not the first (missing .additionalProperties ).")

# Compare the two JSON schemas
compare_json_schemas(schema1, schema2)


# COMMAND ----------

print("Missing Keys:")
for diff in missing_keys:
    print(diff)

# COMMAND ----------

print("Type Mismatches:")
for diff in type_mismatches:
    print(diff)

# COMMAND ----------

print("Format Errors:")
for diff in format_errors:
    print(diff)

# COMMAND ----------

print("AdditionalProperties Errors:")
for diff in additional_properties_errors:
    print(diff)