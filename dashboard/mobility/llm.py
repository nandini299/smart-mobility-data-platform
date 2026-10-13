import os

from dotenv import load_dotenv
from openai import OpenAI
from schema import GOLD_SCHEMA, SENSOR_SCHEMA

load_dotenv()

MODEL = os.getenv("OPENROUTER_MODEL", "openrouter/free")

_client = None


def get_client():
    """Create the OpenRouter client on first use.

    Raising here (instead of at import time) lets the dashboard start and
    show a readable error when the API key is missing.
    """

    global _client

    if _client is None:
        api_key = os.getenv("OPENROUTER_API_KEY")

        if not api_key:
            raise ValueError("OPENROUTER_API_KEY not found")

        _client = OpenAI(
            base_url="https://openrouter.ai/api/v1",
            api_key=api_key,
        )

    return _client


def detect_schema(question):
    """
    Select the appropriate schema based on the user's question.
    """

    question_lower = question.lower()

    sensor_keywords = [
        "sensor",
        "sensors",
        "reading",
        "readings",
        "firmware",
        "temperature",
        "visibility",
        "co_ppm",
        "traffic count",
        "traffic_count",
        "air quality",
        "hourly sensor",
    ]

    if any(keyword in question_lower for keyword in sensor_keywords):
        return SENSOR_SCHEMA

    return GOLD_SCHEMA


def generate_sql(question):
    """
    Generate Athena SQL using the schema relevant to the question.
    """

    selected_schema = detect_schema(question)

    prompt = f"""
You are a SQL generator for an AWS Athena data warehouse.

Use the following schema for this question:

{selected_schema}

Your job is to convert the user's natural-language question
into a valid AWS Athena SQL query.

Rules:
- Return ONLY SQL.
- Do not return markdown.
- Do not use SQL code fences.
- Only generate SELECT queries.
- Only use the tables and columns provided in the selected schema.
- Use fully qualified table names, such as
  mobility_gold.fact_trips or mobility_gold.fact_sensor_hourly.
- Do not invent tables or columns.
- Do not modify any data.
- For sensor questions, use only the sensor tables.
- For taxi questions, use only the taxi tables.
- Use appropriate JOIN conditions when multiple tables are needed.
- Do not add a LIMIT unless it is useful for the question.

USER QUESTION:
{question}
"""

    response = get_client().chat.completions.create(
        model=MODEL,
        messages=[
            {
                "role": "user",
                "content": prompt,
            }
        ],
    )

    sql = response.choices[0].message.content

    if not sql:
        raise ValueError("The model returned an empty SQL query.")

    return sql.strip()
