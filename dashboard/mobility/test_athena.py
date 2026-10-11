from athena import run_query
from llm import generate_sql
from sql_validator import validate_sql

question = "What was the total revenue for each month in 2023?"

print("\nUser question:")
print(question)


# 1. Generate SQL
sql = generate_sql(question)

print("\nGenerated SQL:")
print(sql)


# 2. Validate SQL
valid, message = validate_sql(sql)

print("\nValidation:")
print(message)


if not valid:
    raise ValueError("SQL validation failed.")


# 3. Execute in Athena
df = run_query(sql)

print("\nAthena result:")
print(df)
