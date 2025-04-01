# tests/conftest.py
import pytest
import os
import psycopg2
from psycopg2 import Error, sql, connect


@pytest.fixture(scope="session")
def openGauss_connection():
    """Create a test database connection."""
    try:
        connection = connect(
            host=os.getenv("OPENGAUSS_HOST", "0.0.0.0"),
            port=os.getenv("OPENGAUSS_PORT", "4432"),
            user=os.getenv("OPENGAUSS_USER", "root"),
            password=os.getenv("OPENGAUSS_PASSWORD", "testpassword"),
            dbname=os.getenv("OPENGAUSS_DATABASE", "test_db")
        )

        if  connection.closed == 0:
            # Create a test table
            cursor = connection.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS test_table (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    name VARCHAR(255),
                    value INT
                )
            """)
            connection.commit()

            yield connection

            # Cleanup
            cursor.execute("DROP TABLE IF EXISTS test_table")
            connection.commit()
            cursor.close()
            connection.close()

    except Error as e:
        pytest.fail(f"Failed to connect to OpenGauss: {e}")


@pytest.fixture(scope="session")
def openGauss_cursor(openGauss_connection):
    """Create a test cursor."""
    cursor = openGauss_connection.cursor()
    yield cursor
    cursor.close()