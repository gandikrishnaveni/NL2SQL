import pandas as pd
from nlp_engine import NLPEngine
from rbac_manager import is_authorized  # <-- import your RBAC

def run_complete_demo():
    db_path = "../data/college_2.sqlite"
    engine = NLPEngine(db_path)

    print("=== LegalBrain AI: End-to-End Demo ===")
    print(f"Connected to: {db_path}")

    # 🔐 Step 0: Login
    emp_id = input("Enter Employee ID (E001/E002/E003): ")

    while True:
        user_input = input("\n[Enter Command] (or type 'exit' to quit): ")

        if user_input.lower() == 'exit':
            break

        # --- Step 1: Clarification ---
        print("\n--- Step 1: Checking Intent Clarity ---")
        status = engine.get_clarification(user_input)

        if "AMBIGUOUS" in status:
            print(f"🤖 AI needs clarification: {status.replace('AMBIGUOUS:', '').strip()}")
            clarification = input("Please clarify your request: ")
            user_input = user_input + " based on " + clarification

        else:
            print("✅ Intent is clear. Proceeding...")

        # --- Step 2: Generate SQL ---
        print("\n--- Step 2: NLP Generation ---")
        generated_sql = engine.generate_sql(user_input)
        print(f"Generated SQL: {generated_sql}")

        # 🔐 --- NEW STEP 3: RBAC CHECK ---
        if not is_authorized(emp_id, generated_sql):
            print("❌ RBAC BLOCKED: You do not have permission for this operation.")
            continue
        else:
            print("✅ RBAC Passed")

        # --- Step 4: Safety Check ---
        if any(word in generated_sql.upper() for word in ["DELETE", "UPDATE", "DROP"]):
            confirm = input("⚠️ DANGER: This query modifies data. Run it? (y/n): ")
            if confirm.lower() != 'y':
                print("Execution cancelled.")
                continue

        # --- Step 5: Execute ---
        print("\n--- Step 3: Database Execution ---")
        result = engine.execute_query(generated_sql)

        # --- Step 6: Display ---
        if isinstance(result, pd.DataFrame):
            if result.empty:
                print("Query successful, but no data was found.")
            else:
                print("Results Found:")
                print(result.to_string(index=False))
        else:
            print(result)

if __name__ == "__main__":
    run_complete_demo()
