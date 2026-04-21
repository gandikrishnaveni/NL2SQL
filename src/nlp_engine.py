import os
import sqlite3
import re
import pandas as pd
from dotenv import load_dotenv
from langchain_ollama import ChatOllama
from langchain_core.prompts import ChatPromptTemplate

# Load environment variables
load_dotenv()

class NLPEngine:
    def __init__(self, db_path):
        self.db_path = db_path
        self.llm = ChatOllama(
            model="qwen2.5-coder:1.5b",
            temperature=0,  
            base_url="http://localhost:11434"
        )

    def get_database_schema(self):
        """Extracts schema. If file doesn't exist yet, returns empty for new DB scenarios."""
        try:
            if not os.path.exists(self.db_path): return ""
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute("SELECT sql FROM sqlite_master WHERE type='table';")
            schemas = [row[0] for row in cursor.fetchall() if row[0]]
            conn.close()
            return "\n\n".join(schemas)
        except Exception as e:
            return f"Error reading schema: {str(e)}"

    def get_clarification(self, user_query):
        """
        Universal Ambiguity Check: 
        Triggers if subjective terms are used without a column.
        """
        schema = self.get_database_schema()
        
        clarification_template = """<|im_start|>system
You are a SQL Architect. Analyze the user query against the SCHEMA.
1. If 'high', 'best', 'top', or 'values' is used and multiple numeric columns exist, respond AMBIGUOUS.
2. If the query clearly maps to a column or is a CREATE/INSERT action, respond CLEAR.
3. If 'values' is used without a column name, it is ALWAYS AMBIGUOUS.

SCHEMA:
{schema}
<|im_end|>
<|im_start|>user
{question}
<|im_end|>
<|im_start|>assistant
"""
        prompt = ChatPromptTemplate.from_template(clarification_template)
        chain = prompt | self.llm
        return chain.invoke({"schema": schema, "question": user_query}).content.strip()

    def generate_sql(self, user_query):
        """
        Generalized SQL Generator with Intent Logic and Syntax Guards.
        """
        schema = self.get_database_schema()

        system_template = """<|im_start|>system
You are an expert SQLite Translator. Logic rules:
1. INTENT: "Show/List/Who" -> SELECT. "Total/Sum" -> SUM(). "How many" -> COUNT().
2. SORTING: If "top/best/high", use ORDER BY [col] DESC LIMIT [N].
3. SQLITE RULES: NEVER use 'CREATE DATABASE' or 'USE'. Use 'CREATE TABLE IF NOT EXISTS'.
4. Output ONLY the SQL code. No markdown. No explanation.

SCHEMA:
{schema}
<|im_end|>
<|im_start|>user
{question}
<|im_end|>
<|im_start|>assistant
"""
        prompt = ChatPromptTemplate.from_template(system_template)
        chain = prompt | self.llm
        
        try:
            response = chain.invoke({"schema": schema, "question": user_query})
            return re.sub(r'```sql|```', '', response.content.strip()).strip()
        except Exception as e:
            return f"NLP Error: {str(e)}"

    def execute_query(self, sql_query, user_command=None):
        """Python-side file handling and SQL execution safety."""
        try:
            # 1. HANDLE NEW DATABASE FILE CREATION
            if user_command and any(word in user_command.lower() for word in ["create database", "new database"]):
                match = re.search(r'(?:database|named)\s+(\w+)', user_command.lower())
                if match:
                    new_db_name = match.group(1)
                    self.db_path = f"data/{new_db_name}.sqlite"
                    os.makedirs('data', exist_ok=True)
                    sqlite3.connect(self.db_path).close()
                    print(f"📁 Initializing/Switching to: {self.db_path}")

            # 2. SQL SYNTAX GUARD (Removes incompatible MySQL commands)
            clean_statements = []
            for stmt in sql_query.split(';'):
                s = stmt.strip()
                if not any(bad in s.upper() for bad in ["CREATE DATABASE", "USE "]) and s:
                    clean_statements.append(s)

            # 3. DB EXECUTION
            conn = sqlite3.connect(self.db_path)
            # Check if we are doing a SELECT or an Action
            if clean_statements and clean_statements[0].upper().startswith("SELECT"):
                df = pd.read_sql_query("; ".join(clean_statements), conn)
                conn.close()
                return df if not df.empty else "⚠️ No results found."
            else:
                cursor = conn.cursor()
                for sql in clean_statements:
                    cursor.execute(sql)
                conn.commit()
                res = f"✅ Success! Executed in {self.db_path}"
                conn.close()
                return res
        except Exception as e:
            return f"Execution Error: {str(e)}"

if __name__ == "__main__":
    os.makedirs('data', exist_ok=True)
    engine = NLPEngine("data/college_2.sqlite")
    print("--- ⚖️ LegalBrain AI: Final Unified Engine ---")
    while True:
        query = input("\n[Enter Command]: ")
        if query.lower() == 'exit': break
        
        status = engine.get_clarification(query)
        if "AMBIGUOUS" in status:
            print(f"🤔 {status}")
            query = input("Please specify: ")
            
        sql = engine.generate_sql(query)
        print(f"🚀 SQL: {sql}")
        
        result = engine.execute_query(sql, user_command=query)
        print("\n--- RESULTS ---")
        print(result)