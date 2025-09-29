import mysql.connector

# Connect to the MySQL server
connection = mysql.connector.connect(
    host="mysql_container",  # Container name of MySQL
    user="root",
    password="password",
    database="Metadata"
)

# Execute your queries here
cursor = connection.cursor()
table_name = "ShardT"
create_table_query = f"CREATE TABLE {table_name} ( Stud_id_low INT, Shard_id INT, Shard_size INT, Valid_idx INT, Update_idx INT);"
# print(create_table_query)
cursor.execute(create_table_query)

table_name = "MapT"
create_table_query = f"CREATE TABLE {table_name} ( Shard_id INT, Server_id INT);"
# print(create_table_query)
cursor.execute(create_table_query)
connection.commit()
