def validate_query(query: str):
    """
    Analyze SQL query and determine risk level.
    Returns a dictionary with decision and reason.
    """

    if not query:
        return {
            "allowed": False,
            "risk_level": "high",
            "reason": "Empty query."
        }

    query_clean = query.strip().lower()

    # Block multiple statements (basic SQL injection protection)
    if ";" in query_clean[:-1]:
        return {
            "allowed": False,
            "risk_level": "high",
            "reason": "Multiple SQL statements detected."
        }

    # Safe queries
    if query_clean.startswith("select"):
        return {
            "allowed": True,
            "risk_level": "low",
            "reason": "Read-only query."
        }

    # Risky but allowed with confirmation
    if query_clean.startswith(("update", "delete", "insert")):

        # Extra protection: block UPDATE/DELETE without WHERE
        if query_clean.startswith(("update", "delete")) and "where" not in query_clean:
            return {
                "allowed": False,
                "risk_level": "high",
                "reason": "UPDATE/DELETE without WHERE clause."
            }

        return {
            "allowed": True,
            "risk_level": "medium",
            "reason": "Data modification query. Confirmation required."
        }

    # Dangerous queries
    if query_clean.startswith(("drop", "truncate", "alter")):
        return {
            "allowed": False,
            "risk_level": "high",
            "reason": "Dangerous schema modification query."
        }

    return {
        "allowed": False,
        "risk_level": "unknown",
        "reason": "Unrecognized query type."
    }
