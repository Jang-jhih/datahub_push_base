from push_base.lineage import *
from push_base.ProcessManageFile import *


if __name__ == '__main__':

    #%%
    # host = "104.199.190.117"
    host = '34.80.155.138'
    port = 8080
    gms_server=f"http://{host}:{port}"
    #%%
    csv_file_name = os.path.join('manage','government.csv')
    df_and_key = read_manage_file(csv_file_name)
    keys = df_and_key[1]
    df = df_and_key[0]
    
    frequency = 0
    for key in keys:

        frequency += 1
        df_tmp = df[df['key'] == key]
        
        dataset_object = manager(df_tmp)  
        storage = dataset_object[0]
        table = dataset_object[1]
        fields_schema =dataset_object[2]
        primary_keys = dataset_object[3]
        
        # 組合物件為event
        dataset_event = create_dataset_object(database = storage,
                                           table = table,
                                           fields = fields_schema,
                                           primary_keys = primary_keys)
            
        # 發出post
        rest_emitter = DatahubRestEmitter(gms_server=gms_server)
        rest_emitter.emit(dataset_event)
        
        print('-------------------------------------------------------')
        print(f'建立dataset {key}')
        # 備份json 檔
        path = f'dataset_{key}'
        save_to_json(path=path,event=dataset_event)
        
        # 組合物件為event
        lineageMcp = field_lineages(df = df_tmp,
                                   downstream_database = storage,
                                   downstream_table = table)
            
        path = f'lineage_{key}'
        save_to_json(path=path,event=lineageMcp)
        rest_emitter.emit_mcp(lineageMcp)
        
    print(f"共 {frequency} 筆 dataset")
        
            
    
