"""
This is the entrypoint to the program. 'python main.py' will be executed and the 
expected csv file should exist in ../data/destination/ after the execution is complete.
"""
import csv
from src.some_storage_library import SomeStorageLibrary




if __name__ == '__main__':
    """Entrypoint"""
    print('Beginning the ETL process...')
    #read SOURCECOLUMNS.txt and SOURCEDATA.txt if they exist
    try:
        list_cols = []
        with open('data/source/SOURCECOLUMNS.txt', newline='') as csvfile:
            colsreader = csv.reader(csvfile, delimiter='|')
            for row in colsreader:
                list_cols.append(row)

        list_data =[]
        with open('data/source/SOURCEDATA.txt', newline='') as csvfile:
            datareader = csv.reader(csvfile, delimiter='|')
            for row in datareader:
                list_data.append(row)
    except Exception as e:
        raise e
    print("Read the SOURCECOLUMNS.txt and SOURCEDATA.txt files")

    #sort the columns by value-- reflects the order in SOURCEDATA.txt
    sorted_cols = sorted(list_cols, key=lambda x: int(x[0]) )
    sorted_cols = [i[1] for i in sorted_cols]

    #creates the csv output file
    with open('OUTPUTDATA.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(sorted_cols)
        writer.writerows(list_data)
    print('Done writing the OUTPUTDATA.csv file')

    #moves the created csv file to relevant location
    call = SomeStorageLibrary()
    try:
        call.load_csv("OUTPUTDATA.csv")
    except Exception as e:
        raise e