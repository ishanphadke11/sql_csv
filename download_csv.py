import requests
import sys
import os
import subprocess
import duckdb

def download_csv(url, save_path):
    try:
        response = requests.get(url)
        response.raise_for_status() # make sure no errors were encountered for the request
        file_name = os.path.basename(url) # get the name of the csv file

        if not file_name.endswith('.csv'):
            file_name += '.csv' # if the file does not end with .csv add it on at the end

        file_path = os.path.join(save_path, file_name) # create a path for the downlaoded file

        
        with open(file_path, 'wb') as file:
            file.write(response.content)
        return file_path
    
    except requests.exceptions.RequestException as e:
        print(f"Failed to download CSV file: {e}")
        sys.exit(1)

def run_cpp(csv_file, parquet_file, makefile_dir):
    try:
        subprocess.run(['make', '-C', makefile_dir], check=True)  # compile cpp program
        # execute the cpp program
        subprocess.run([os.path.join(makefile_dir, 'CSV_PARQUET'), csv_file, parquet_file], check=True)

    except subprocess.CalledProcessError as e:
        print(f"failed to run the cpp program: {e}")
        sys.exit(1)

def run_queries(parquet_file, queries_file):
    conn = duckdb.connect() 

    with open(queries_file, 'r') as file:
        queries = file.readlines() # read the queries

    for query in queries:
        query = query.strip()  # get rid of trailing whitespaces
        if query:
            parquet_query = query.replace("parquet_file", f"read_parquet('{parquet_file}')")
            print(f"{parquet_query}")  # print the query
            try:
                result = conn.execute(parquet_query).fetchall()
                print(f"Result: {result}") 

            except Exception as e:
                print(f"Failed to execute query: {e}")
    conn.close()

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print(f"Usage: {sys.argv[0]} <csv_url> <queries_file>")
        sys.exit(1)

    csv_url = sys.argv[1]  # first command line argument is the url
    queries_file = sys.argv[2]  # second command line argument is the queries text file

    save_folder = os.path.dirname(os.path.abspath(__file__))  # define the save folder as the current folder

    csv_file = download_csv(csv_url, save_folder)  #download the csv file

    parquet_file = os.path.join(save_folder, 'output.parquet')  # create the output parquet file

    makefile_dir = save_folder  # define the makefile directory as the save folder (current folder)

    run_cpp(csv_file, parquet_file, makefile_dir)  #run cpp program

    run_queries(parquet_file, queries_file)  # run the queries
