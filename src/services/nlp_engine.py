# services/nlp_engine.py

def generate_sql_from_nl(nl_query: str) -> str:
    """
    Placeholder NLP logic.
    Replace with your NL-to-SQL model later.
    """

    nl = nl_query.lower()

    if "delete" in nl:
        return "DELETE FROM employees WHERE id = 1;"
    elif "update" in nl:
        return "UPDATE employees SET salary = salary + 1000 WHERE id = 1;"
    elif "show" in nl or "select" in nl:
        return "SELECT * FROM employees;"
    else:
        return "SELECT * FROM employees;"
