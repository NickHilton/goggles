import os

if os.environ.get("ENV") == "PRODUCTION":
    INSERT_TABLE = "nonup"
else:
    INSERT_TABLE = "nonup_test"
