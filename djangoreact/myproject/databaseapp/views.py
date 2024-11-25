from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
import json
import snowflake.connector
import pypyodbc as odbc

@csrf_exempt
def snowflake_login(request):
    if request.method == "POST":
        try:
            # Parse the request body
            data = json.loads(request.body)
            username = data.get("username")
            password = data.get("password")
            account = data.get("account")
            warehouse = data.get("warehouse")
            database = data.get("database")
            schema = data.get("schema")

            # Validate input fields
            missing_fields = [
                field for field in ["username", "password", "account", "warehouse", "database", "schema"]
                if not data.get(field)
            ]
            if missing_fields:
                return JsonResponse(
                    {"message": f"Missing required fields: {', '.join(missing_fields)}"}, status=400
                )

            # Attempt to connect to Snowflake
            conn = snowflake.connector.connect(
                user=username,
                password=password,
                account=account,
                warehouse=warehouse,
                database=database,
                schema=schema
            )

            # Perform a simple query to verify the connection
            conn.cursor().execute("SELECT CURRENT_USER();")
            conn.close()

            # Return success response
            return JsonResponse({"message": "Login successful!"}, status=200)

        except snowflake.connector.errors.DatabaseError as e:
            error_code = e.errno
            error_message = str(e)

            # Handle specific Snowflake errors
            if error_code == 2002:
                return JsonResponse({"message": "Authentication failed. Check your username or password."}, status=401)
            elif error_code == 250001:
                return JsonResponse({"message": "Connection timeout. Verify your account details and network."}, status=408)
            else:
                return JsonResponse({"message": f"Snowflake error: {error_message}"}, status=400)

        except json.JSONDecodeError:
            # Handle invalid JSON payloads
            return JsonResponse({"message": "Invalid JSON format in request body."}, status=400)

        except KeyError as e:
            # Handle missing keys in the JSON
            return JsonResponse({"message": f"Missing key in request: {str(e)}"}, status=400)

        except Exception as e:
            # Handle all other exceptions
            return JsonResponse({"message": f"An unexpected error occurred: {str(e)}"}, status=500)

    # If the request method is not POST
    return JsonResponse({"message": "Invalid request method. Use POST."}, status=405)



@csrf_exempt
def SSMS_Login(request):
    if request.method == 'POST':
        # Get the JSON data from the request body
        data = json.loads(request.body)
        SERVER_NAME = data.get('server_name')
        USER = data.get('username')
        PASSWORD = data.get('password')
        DATABASE_NAME = 'master'  # Use the 'master' database to query for all databases

        try:
            # Connection string
            connection_string = f"""
            DRIVER={{SQL SERVER}};
            SERVER={SERVER_NAME};
            DATABASE={DATABASE_NAME};
            USER={USER};
            PASSWORD={PASSWORD};

            Trust_Connection=yes;
            """

            # Establish the database connection
            conn = odbc.connect(connection_string)
            cursor = conn.cursor()

            # Execute query to get all databases
            cursor.execute("SELECT name FROM sys.databases")
            databases = cursor.fetchall()

            databases_with_tables = []

            # Iterate through each database to fetch its tables
            for db in databases:
                db_name = db[0]
                try:
                    # Switch to the database
                    cursor.execute(f"USE [{db_name}]")
                    # Query to fetch all tables in the database
                    cursor.execute("""
                        SELECT TABLE_NAME
                        FROM INFORMATION_SCHEMA.TABLES
                        WHERE TABLE_TYPE = 'BASE TABLE'
                    """)
                    tables = cursor.fetchall()
                    # Add database and its tables to the result
                    databases_with_tables.append({
                        'database': db_name,
                        'tables': [table[0] for table in tables]
                    })
                except Exception as e:
                    # Handle any errors when querying tables for a specific database
                    databases_with_tables.append({
                        'database': db_name,
                        'tables': [],
                        'error': str(e)  # Include the error for debugging
                    })

            # Close the cursor and connection
            cursor.close()
            conn.close()

            # Return the databases and tables as JSON
            return JsonResponse(databases_with_tables, safe=False)

        except Exception as e:
            # Handle any connection errors
            return JsonResponse({'error': str(e)}, status=500)

    return JsonResponse({'error': 'Invalid request method'}, status=400)