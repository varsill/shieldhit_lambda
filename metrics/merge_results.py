import pickle
import pandas as pd
import os

def load(input_file_path):
    if not os.path.isfile(input_file_path):
        return []
    with open(input_file_path, "rb") as input_file:
        results = pickle.load(input_file)
    return results

def save(result_dump, output_file_path):
    with open(output_file_path, "wb") as output_file:
      pickle.dump(result_dump, output_file)

def merge_with_override(first_filename, second_filename, result_filename):
    first_dump = load(first_filename)
    second_dump = load(second_filename)

    df1 = pd.DataFrame(first_dump)
    df2 = pd.DataFrame(second_dump)
    
    result_df = df1.combine_first(df2)
    result_dump = result_df.values.tolist()
    save(result_dump, result_filename)

def append(filename, result):
    previous_dump = load(filename)
    final_result_dump = previous_dump+[result]
    save(final_result_dump, filename)
