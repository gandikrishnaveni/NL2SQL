import os

# Get project root (src folder)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Create audit.db inside database folder ONLY
DB_PATH = os.path.join(BASE_DIR, "audit.db")
