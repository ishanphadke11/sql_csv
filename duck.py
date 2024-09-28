import duckdb

parquet_file = 'output.parquet'

queries_file = 'queries.txt'

conn = duckdb.connect()

with open(queries_file, 'r') as file:
    queries = file.readlines()  # read the queries

for query in queries:
    if query:
        # reformat the query to replace the words 'parquet_file' with the 'read_parquet' command
        parquet_query = query.replace("parquet_file", f"read_parquet('{parquet_file}')") 
        print(f"{parquet_query}")  # print the query
        result = conn.execute(parquet_query).fetchall()  # fetch all rows from the result of the query
        print(result)  # print the result

conn.close()