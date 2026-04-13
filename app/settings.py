import os
from dotenv import load_dotenv

load_dotenv()

# IBM i
IBMI_HOST = os.getenv("IBMI_HOST")
IBMI_USER = os.getenv("IBMI_USER")
IBMI_PASSWORD = os.getenv("IBMI_PASSWORD")
IBMI_JDBC_URL = os.getenv("IBMI_JDBC_URL")
JT400_JAR = os.getenv("JT400_JAR")

# Librerías
IBMI_SOURCE_LIBRARY = os.getenv("IBMI_SOURCE_LIBRARY")
IBMI_TARGET_LIBRARY = os.getenv("IBMI_TARGET_LIBRARY")

# Tablas
IBMI_SOURCE_TABLE = os.getenv("IBMI_SOURCE_TABLE")
IBMI_TARGET_TABLE = os.getenv("IBMI_TARGET_TABLE")
