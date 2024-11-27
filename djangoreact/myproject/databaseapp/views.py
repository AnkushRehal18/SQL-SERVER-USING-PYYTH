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
            selected_database = data.get("selected_database")  # New field
            selectedSchema = data.get("selectedSchema")

            # Validate input fields
            missing_fields = [
                field for field in ["username", "password", "account", "warehouse"]
                if not data.get(field)
            ]
            if missing_fields:
                return JsonResponse(
                    {"message": f"Missing required fields: {', '.join(missing_fields)}"}, status=400
                )

            # Connect to Snowflake
            conn = snowflake.connector.connect(
                user=username,
                password=password,
                account=account,
                warehouse=warehouse
            )
            cursor = conn.cursor()

            if selected_database:
                # Fetch schemas for the selected database
                cursor.execute(f"SHOW SCHEMAS IN DATABASE {selected_database}")
                schemas = cursor.fetchall()
                schema_names = [row[1] for row in schemas]
                conn.close()
                return JsonResponse(
                    {"message": "Schemas fetched successfully!", 
                    "schemas": schema_names}, status=200
                )
            else:
                # Fetch the list of databases
                cursor.execute("SHOW DATABASES")
                databases = cursor.fetchall()
                database_names = [row[1] for row in databases]
                conn.close()
                return JsonResponse(
                    {"message": "Login successful!", "databases": database_names}, status=200
                )

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
            return JsonResponse({"message": "Invalid JSON format in request body."}, status=400)

        except KeyError as e:
            return JsonResponse({"message": f"Missing key in request: {str(e)}"}, status=400)

        except Exception as e:
            return JsonResponse({"message": f"An unexpected error occurred: {str(e)}"}, status=500)

    return JsonResponse({"message": "Invalid request method. Use POST."}, status=405)



@csrf_exempt
def SSMS_Login_And_FetchData(request):
    print("SSMS_Login_And_FetchData called at 77")
    if request.method == 'POST':
        try:
            # Parse JSON input from the frontend
            data = json.loads(request.body)
            print('data at 82',data)

            # Extract parameters
            server_name = data.get('server_name')
            username = data.get('username')
            password = data.get('password')
            selected_database = data.get('selected_database')
            selected_table = data.get('selected_table')

            # Handle login request and fetch databases
            if server_name and username and password:
                # Connection string for the 'master' database
                connection_string = f"""
                    DRIVER={{SQL SERVER}};
                    SERVER={server_name};
                    DATABASE=master;
                    UID={username};
                    PWD={password};
                    Trust_Connection=no;
                """
                try:
                    conn = odbc.connect(connection_string)
                    cursor = conn.cursor()

                    # Fetch all databases
                    cursor.execute("SELECT name FROM sys.databases")
                    databases = [db[0] for db in cursor.fetchall()]

                    # Close the connection
                    cursor.close()
                    conn.close()

                    # Store connection details in the session
                    request.session['connection_details'] = {
                        'server_name': server_name,
                        'username': username,
                        'password': password
                    }

                    if not selected_database:
                            return JsonResponse({
                                'success': True,
                                'message': 'Login successful!',
                                'databases': databases,
                            })
                    

                except odbc.Error as e:
                    return JsonResponse({'error': f'Login failed: {str(e)}'}, status=401)

            # Check for valid session if login is not part of the request
            connection_details = request.session.get('connection_details')
            if not connection_details:
                return JsonResponse({'error': 'No valid connection found. Please log in first.'}, status=400)

            # Handle database selection to fetch tables or data
            print('134',selected_database)
            if selected_database:
                # Connection string for the selected database
                print('136',selected_database)
                connection_string = f"""
                    DRIVER={{SQL SERVER}};
                    SERVER={connection_details['server_name']};
                    DATABASE={selected_database};
                    UID={connection_details['username']};
                    PWD={connection_details['password']};
                    Trust_Connection=no;
                """
                try:
                    conn = odbc.connect(connection_string)
                    cursor = conn.cursor()

                    if not selected_table:
                        # Fetch all tables in the selected database
                        cursor.execute("""
                            SELECT TABLE_NAME
                            FROM INFORMATION_SCHEMA.TABLES
                            WHERE TABLE_TYPE = 'BASE TABLE'
                        """)
                        tables = [table[0] for table in cursor.fetchall()]

                        cursor.close()
                        conn.close()
                        return JsonResponse({
                            'database': selected_database,
                            'tables': tables
                        })
                    else:
                        # Validate if the table exists
                        cursor.execute("""
                            SELECT TABLE_NAME
                            FROM INFORMATION_SCHEMA.TABLES
                            WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_NAME = ?
                        """, (selected_table,))
                        table_exists = cursor.fetchone()

                        if not table_exists:
                            return JsonResponse({'error': f"Table '{selected_table}' does not exist."}, status=400)

                        # Fetch data from the selected table
                        cursor.execute(f"SELECT * FROM {selected_table}")
                        rows = cursor.fetchall()
                        columns = [column[0] for column in cursor.description]
                        data = [dict(zip(columns, row)) for row in rows]

                        cursor.close()
                        conn.close()
                        return JsonResponse({
                            'columns': columns,
                            'data': data
                        })

                except odbc.Error as e:
                    return JsonResponse({'error': f'Error fetching data: {str(e)}'}, status=500)

            return JsonResponse({'error': 'No login data or fetch data parameters provided.'}, status=400)

        except json.JSONDecodeError:
            return JsonResponse({'error': 'Invalid JSON format.'}, status=400)
        except Exception as e:
            return JsonResponse({'error': str(e)}, status=500)

    return JsonResponse({'error': 'Invalid request method.'}, status=405)