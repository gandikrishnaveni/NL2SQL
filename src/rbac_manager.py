import re

def _contains_keyword(sql: str, keywords: list) -> bool:
    """Helper to check for exact SQL keywords using regex boundaries."""
    sql_upper = sql.upper()
    for kw in keywords:
        if re.search(rf'\b{kw}\b', sql_upper):
            return True
    return False

def is_authorized(emp_id: str, sql: str) -> bool:
    """
    Evaluates if the given employee ID is authorized to execute the generated SQL.
    
    Roles:
    - E001 (Admin)    : All operations allowed
    - E002 (Manager)  : SELECT and UPDATE allowed
    - E003 (Employee) : SELECT only (Strict Read-Only)
    """
    emp_id = emp_id.strip().upper()

    # Define our restricted keyword groups
    # Notice 'UPDATE' is kept separate so we can allow it for E002
    STRICT_DML = ['DELETE', 'INSERT', 'DROP', 'ALTER', 'CREATE', 'TRUNCATE', 'REPLACE']
    UPDATE_DML = ['UPDATE']

    # 1. E001 (Admin): Can do absolutely everything
    if emp_id == 'E001':
        return True

    # 2. E002 (Manager): Can do SELECT and UPDATE. 
    # Must block all other structural/destructive DML.
    if emp_id == 'E002':
        if _contains_keyword(sql, STRICT_DML):
            return False
        return True

    # 3. E003 (Employee): Strict Read-Only. 
    # Must block UPDATE and all other DML.
    if emp_id == 'E003':
        if _contains_keyword(sql, STRICT_DML) or _contains_keyword(sql, UPDATE_DML):
            return False
        return True

    # Default fallback for unknown IDs: Safest approach is Read-Only
    if _contains_keyword(sql, STRICT_DML) or _contains_keyword(sql, UPDATE_DML):
        return False
        
    return True