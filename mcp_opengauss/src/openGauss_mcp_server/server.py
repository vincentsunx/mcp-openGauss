import asyncio
import logging
import os
import psycopg2
from psycopg2 import Error, sql, connect
from mcp.server import Server
from mcp.types import Resource, Tool, TextContent
from pydantic import AnyUrl

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("openGauss_mcp_server")

def get_db_config():
    """Get database configuration from environment variables."""
    config = {
        "host": os.getenv("", "localhost"),
        "port": int(os.getenv("OPENGAUSS_PORT", "5432")), 
        "user": os.getenv("OPENGAUSS_USER"),
        "password": os.getenv("OPENGAUSS_PASSWORD"),
        "dbname": os.getenv("OPENGAUSS_DBNAME"),
    }
    if not all([config["user"], config["password"], config["dbname"]]):
        raise ValueError("Missing required database configuration")
    
    return config

# Initialize server
app = Server("openGauss_mcp_server")

@app.list_resources()
async def list_resources() -> list[Resource]:
    """List openGauss tables as resources."""
    config = get_db_config()
    try:
        with connect(**config) as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT tablename FROM pg_tables WHERE schemaname = 'public'")
                tables = cursor.fetchall()
                logger.info(f"Found tables: {tables}")

                resources = []
                for table in tables:
                    resources.append(
                        Resource(
                            uri=f"openGauss://{table[0]}/data",
                            name=f"Table: {table[0]}",
                            mimeType="text/plain",
                            description=f"Data in table: {table[0]}"
                        )
                    )
                return resources
    except Error as e:
        logger.error(f"Failed to list resources: {str(e)}")
        return []

@app.read_resource()
async def read_resource(uri: AnyUrl) -> str:
    """Read table contents."""
    config = get_db_config()
    uri_str = str(uri)
    logger.info(f"Reading resource: {uri_str}")
    
    if not uri_str.startswith("openGauss://"):
        raise ValueError(f"Invalid URI scheme: {uri_str}")
        
    parts = uri_str[8:].split('/')
    table = parts[0]
    
    try:
        with connect(**config) as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"SELECT * FROM {table} LIMIT 100")
                columns = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                result = [",".join(map(str, row)) for row in rows]
                return "\n".join([",".join(columns)] + result)
                
    except Error as e:
        logger.error(f"Database error reading resource {uri}: {str(e)}")
        raise RuntimeError(f"Database error: {str(e)}")

@app.list_tools()
async def list_tools() -> list[Tool]:
    """List available openGauss tools."""
    logger.info("Listing tools...")
    return [
        Tool(
            name="execute_sql",
            description="Execute an SQL query on the openGauss server",
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "The SQL query to execute"
                    }
                },
                "required": ["query"]
            }
        )
    ]


def handle_meta_command(cursor, query: str, config: dict) -> list[TextContent]:
    """Handle OpenGauss meta-commands (e.g., \d, \dt)."""
    command = query.strip()
    
    # Handle \d (list tables)
    if command == "\\d":
        cursor.execute("SELECT tablename FROM pg_tables WHERE schemaname = 'public'")
        tables = cursor.fetchall()
        result = ["Tables_in_" + config["dbname"]]  # Header
        result.extend([table[0] for table in tables])
        return [TextContent(type="text", text="\n".join(result))]
    
    # Handle \dt (list tables with details)
    elif command == "\\dt":
        cursor.execute("SELECT tablename, tableowner, tablespace FROM pg_tables WHERE schemaname = 'public'")
        columns = ["Table", "Owner", "Tablespace"]
        rows = cursor.fetchall()
        result = [",".join(columns)]  # Header
        result.extend([",".join(map(str, row)) for row in rows])
        return [TextContent(type="text", text="\n".join(result))]
    
    # Handle \d+ (list tables with extended details)
    elif command == "\\d+":
        cursor.execute("SELECT tablename, tableowner, tablespace, hasindexes, hasrules, hastriggers FROM pg_tables WHERE schemaname = 'public'")
        columns = ["Table", "Owner", "Tablespace", "Has Indexes", "Has Rules", "Has Triggers"]
        rows = cursor.fetchall()
        result = [",".join(columns)]  # Header
        result.extend([",".join(map(str, row)) for row in rows])
        return [TextContent(type="text", text="\n".join(result))]
    
    # Handle \du (list users and roles)
    elif command == "\\du":
        cursor.execute("SELECT rolname, rolsuper, rolinherit, rolcreaterole, rolcreatedb, rolcanlogin FROM pg_roles")
        columns = ["Role", "Superuser", "Inherit", "Create Role", "Create DB", "Can Login"]
        rows = cursor.fetchall()
        result = [",".join(columns)]  # Header
        result.extend([",".join(map(str, row)) for row in rows])
        return [TextContent(type="text", text="\n".join(result))]
    
    # Unsupported meta-command
    else:
        return [TextContent(type="text", text=f"Unsupported meta-command: {command}")]

@app.call_tool()
async def call_tool(name: str, arguments: dict) -> list[TextContent]:
    """Execute SQL commands."""
    config = get_db_config()
    logger.info(f"Calling tool: {name} with arguments: {arguments}")
    
    if name != "execute_sql":
        raise ValueError(f"Unknown tool: {name}")
    
    query = arguments.get("query")
    if not query:
        raise ValueError("Query is required")
    
    try:
        with connect(**config) as conn:
            with conn.cursor() as cursor:
                
                if query.strip().startswith("\\"):
                    return handle_meta_command(cursor, query, config)
                
                # Execute regular SQL queries
                cursor.execute(query)

                # Regular SELECT queries
                if query.strip().upper().startswith("SELECT"):
                    columns = [desc[0] for desc in cursor.description]
                    rows = cursor.fetchall()
                    result = [",".join(map(str, row)) for row in rows]
                    return [TextContent(type="text", text="\n".join([",".join(columns)] + result))]
                
                # Non-SELECT queries
                else:
                    conn.commit()
                    return [TextContent(type="text", text=f"Query executed successfully. Rows affected: {cursor.rowcount}")]
                
    except Error as e:
        logger.error(f"Error executing SQL '{query}': {e}")
        return [TextContent(type="text", text=f"Error executing query: {str(e)}")]

async def main():
    """Main entry point to run the MCP server."""
    from mcp.server.stdio import stdio_server
    
    logger.info("Starting openGauss MCP server...")
    config = get_db_config()
    logger.info(f"Database config: {config['host']}/{config['dbname']} as {config['user']}")
    async with stdio_server() as (read_stream, write_stream):
        try:
            await app.run(
                read_stream,
                write_stream,
                app.create_initialization_options()
            )
        except Exception as e:
            logger.error(f"Server error: {str(e)}", exc_info=True)
            raise

if __name__ == "__main__":
    asyncio.run(main())