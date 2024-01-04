import csv
# import os
# from typing import Final
from pymonad.tools import curry
from pymonad.either import Left, Right
from pymonad.io import IO

# Function to handle file reading
def read_csv_file(file_path):
    def read_file():
        try:
            with open(file_path, 'r') as csvfile:
                    reader = csv.reader(csvfile)
                    data = [row for row in reader]
                    return data
        except:
            return Left("Error: File not found")   
    return IO(read_file)

def remove_header(data):
    try: 
        return (
            Right(data[1:])
            if len(data) > 1 
            else Left("Error: Unable to remove header")
    )
    except:
        return data

@curry(2)
def extract_column(column_index, data):
    try:
        return (
            Right(data.value).bind(lambda rows: 
            Right(list(map(lambda row: row[column_index], rows))))
    )
    except:
        return data
    
extract_score_column = extract_column(1)
extract_name_column = extract_column(0)

def convert_to_float(data):
    try:
        return (
            Right(list(map(float, data.value))) 
            if all(item.isdigit() for item in data.value)
            else Left("Error: Unable to convert to float")
    )
    except:
        return data


def calculate_average(column_values):
    try:
        return  (
            Right(sum(column_values.value) / len(column_values.value)) 
    )
    except:
        return column_values
    
# Data pipeline using the Either monad and custom sequencing operator
csv_file_path = 'example.csv'

# data =  read_csv_file(csv_file_path)

names = (
    read_csv_file(csv_file_path)
    .then (remove_header)
    .then (extract_name_column)
).run()

result = (
    read_csv_file(csv_file_path)
    .then (remove_header)
    .then (extract_score_column)
    .then (convert_to_float)
    .then (calculate_average) 
).run()

if result.is_right() and names.is_right():
    print(f"An average score of {', '.join(names.value[:-1])} and {names.value[-1]} is {result.value}")
else:   
    print(f"Error processing data: {result}")
