
import psycopg2  # Assuming you're using PostgreSQL, you may need to install psycopg2

class Database:
    def __init__(self, db_host, db_port, db_name, db_user, db_password):
        self.db_host = db_host
        self.db_port = db_port
        self.db_name = db_name
        self.db_user = db_user
        self.db_password = db_password

    def connect_to_database(self):
        try:
            # Establish a database connection
            connection = psycopg2.connect(
                host=self.db_host,
                port=self.db_port,
                database=self.db_name,
                user=self.db_user,
                password=self.db_password
            )
            return connection
        except Exception as e:
            print(f"Error connecting to the database: {str(e)}")
            return None

    def insert_data_into_database(self, data, columns, schema_name, table_name):
        values = []
        try:
            # Connect to the database
            connection = self.connect_to_database()
            if connection is not None:
                # Create a cursor for database operations
                cursor = connection.cursor()
                # Set schema on database(PostgreSQL)
                query_set_schema = f"""SET search_path TO {schema_name};"""
                cursor.execute(query_set_schema)

                column_names = ', '.join(columns)
                placeholders = ', '.join(['%s'] * len(columns))
                update_assignments = [f"{col} = excluded.{col}" for col in columns]
                update_clause = ", ".join(update_assignments)
                # Create the SQL query
                create_table_query = f"""
                        CREATE TABLE IF NOT EXISTS {table_name} (
                        id INT PRIMARY KEY,
                        team_name VARCHAR(50) NOT NULL,
                        alias VARCHAR(50),
                        country VARCHAR(50));
                        """
                query = f"""
                    INSERT INTO {table_name} ({column_names})
                    VALUES ({placeholders})
                    ON CONFLICT (id)
                    DO UPDATE SET
                    {update_clause};
                    """

                if data:
                    values = [tuple(row[key] for key in row.keys()) for row in data]
                else:
                    print("No data to insert into the database.")

                def replace_empty_with_none(x):
                    return [(item if item != '' else None) for item in x]

                no_more_emptystring = [replace_empty_with_none(row) for row in values]

                cursor.execute(create_table_query)
                cursor.executemany(query, no_more_emptystring)
                connection.commit()
                cursor.close()
                connection.close()
                print(f"Data inserted into the Database = {self.db_name}, Table = {table_name} successfully.")

        except Exception as e:
            print(f"Error inserting data into the database: {str(e)}")

