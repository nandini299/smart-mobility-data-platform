from llm import generate_sql

question = "What was the total revenue for each month in 2023?"

sql = generate_sql(question)

print("\nGenerated SQL:\n")
print(sql)
