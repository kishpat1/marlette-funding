"""
This is the entrypoint to the program. 'python main.py' will be executed and the 
expected csv file should exist in ../data/destination/ after the execution is complete.
"""
import csv
from src.some_storage_library import SomeStorageLibrary

def read_file(file_path:str):
    print(f"Trying to read the file from {file_path}")
    try:
        list_data = []
        with open(file_path, newline='') as csvfile:
            datareader = csv.reader(csvfile, delimiter='|')
            for row in datareader:
                list_data.append(row)
    except Exception as e:
        raise e
    print(f"Completed reading the file at {file_path}")
    return list_data

def main():
    #read SOURCECOLUMNS.txt and SOURCEDATA.txt if they exist
    list_cols = read_file('data/source/SOURCECOLUMNS.txt')
    list_data = read_file('data/source/SOURCEDATA.txt')

    # sort the columns by value-- reflects the order in SOURCEDATA.txt
    sorted_cols = sorted(list_cols, key=lambda x: int(x[0]))
    sorted_cols = [i[1] for i in sorted_cols]

    # creates the csv output file
    with open('OUTPUTDATA.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(sorted_cols)
        writer.writerows(list_data)
    print('Done writing the OUTPUTDATA.csv file')

    # moves the created csv file to relevant location
    call = SomeStorageLibrary()
    try:
        call.load_csv("OUTPUTDATA.csv")
    except Exception as e:
        raise e



if __name__ == '__main__':
    """Entrypoint"""
    print('Beginning the ETL process...')
    main()
    print('Completed the ETL process')
