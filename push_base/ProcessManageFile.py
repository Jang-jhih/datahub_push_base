import pandas as pd
import os
from push_base.dataset import *
import numpy as np
import chardet

# This function creates a Dataset aspect for a dataset.
def manager(df_tmp):

    my_dict = {
        'field_path': df_tmp['field_path'].to_list(),
        'nativeDataType': df_tmp['nativeDataType'].to_list(),
        'nullable': df_tmp['nullable'].to_list(),
        'recursive': df_tmp['recursive'].to_list(),
        'description': df_tmp['description'].to_list(),
        'tags': df_tmp['tags'].to_list(),
    }


    fields_schema = []
    for i in range(len(my_dict[list(my_dict.keys())[0]])):
        fields_schema.append(create_field_object(
            field_path=my_dict['field_path'][i],
            native_data_type=my_dict['nativeDataType'][i],
            description=my_dict['description'][i],
            nullable=my_dict['nullable'][i],
            recursive=my_dict['recursive'][i],
            tags=my_dict['tags'][i],
        ))

    # 以第一個 row 作為範例，不會有拿錯的情況，要是錯了連 key 都會錯
    storage = list(df_tmp['storage'])[0]
    table = list(df_tmp['database'] + '.' + df_tmp['table'])[0]
    primary_keys = df_tmp[df_tmp['primaryKeys']]['field_path'].tolist()
    return storage, table, fields_schema, primary_keys




# This function creates a Dataset aspect for a dataset.
def save_to_json(path, event):

    # Transform the event to an object
    json_file = post_json_transform(event.to_obj())
    # Save the json file
    json_object = json.dumps(json_file, indent=4)
    # Save the json file
    json_path = os.path.join(os.path.join('manage', 'dataset', 'json', f'{path}.json'))

    with open(os.path.join(json_path), "w") as outfile:
        outfile.write(json_object)






def read_manage_file(csv_file_name):
    """
    Read the manage file into a dataframe
    """
    # Set the data types for the columns
    dtype = {
        'URN_key': str,
        'storage': str,
        'database': str,
        'table': str,
        'field_path': str,
        'nullable': bool,
        'primaryKeys': bool,
        'recursive': bool,
        'tags': str,
        'nativeDataType': str,
        'description': str,
        'lineage_key_1': str,
        'lineage_key_2': str,
        'lineage_key_3': str,
        'lineage_key_4': str,
        'lineage_key_5': str,
        'back': str
    }

    # Read the file into a dataframe
    with open(csv_file_name, 'rb') as f:
        result = chardet.detect(f.read()) 
    encoding = result['encoding']
    df = pd.read_csv(csv_file_name, index_col=False, encoding=encoding, dtype=dtype)
    df.fillna("", inplace=True)

    # Create a key column that is the unique identifier for each row
    keys = df['storage'] + '_' + df['database'] + '_' + df['table']
    df['key'] = keys
    keys = sorted(list(set(keys)))
    return df, keys
