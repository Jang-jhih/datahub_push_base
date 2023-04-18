
import datetime

from datahub.emitter.mce_builder import (
    make_data_platform_urn,
    make_dataset_urn,
    make_tag_urn,
    make_term_urn,
)


import datahub.emitter.mce_builder as builder

from datahub.emitter.serialization_helper import pre_json_transform,post_json_transform
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter

# Imports for metadata model classes
from datahub.metadata.schema_classes import (
    AuditStampClass,
    GlobalTagsClass,
    GlossaryTermAssociationClass,
    GlossaryTermsClass,
    OtherSchemaClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    TagAssociationClass,
    
    BooleanTypeClass, 
    FixedTypeClass, 
    StringTypeClass, 
    BytesTypeClass, 
    NumberTypeClass, 
    DateTypeClass, 
    TimeTypeClass, 
    EnumTypeClass, 
    NullTypeClass, 
    MapTypeClass, 
    ArrayTypeClass, 
    UnionTypeClass, 
    RecordTypeClass

    
)

import pandas as pd
import os
import json


# this function takes a list of values and removes any null values from it
def DropNull(list):
    '''
    This function takes a list of values and removes any null values from it.
    It takes the following parameters:
    - list: The list of values.
    '''
    return [_ for _ in list if pd.notnull(_)]



def selet_field_type(nativeDataType):
    """
    Returns the appropriate class for the given nativeDataType

    nativeDataType: string with the native data type
    """
    nativeDataType = nativeDataType.lower()
    
    type_dict = {
                'boolean': BooleanTypeClass(),
                'fixed': FixedTypeClass(),
                'string': StringTypeClass(),
                'bytes': BytesTypeClass(),
                'number': NumberTypeClass(),
                'date': DateTypeClass(),
                'time': TimeTypeClass(),
                'enum': EnumTypeClass(),
                'null': NullTypeClass(),
                'map': MapTypeClass(),
                'array': ArrayTypeClass(),
                'union': UnionTypeClass(),
                'record': RecordTypeClass()
                }

    return type_dict[nativeDataType]



def get_tags(tags):
    
    TagAssociation = []
    for tag in tags:
        TagAssociation.append(TagAssociationClass(tag=make_tag_urn(tag)))
    return TagAssociation

def fields(fieldPath,nativeDataType,description,nullable,recursive,tags):
    '''
    This function creates the SchemaFieldClass object.
    It takes the following parameters:
    - fieldPath: The path of the field.
    - nativeDataType: The native data type of the field.
    - description: The description of the field.
    - nullable: Whether the field is nullable.
    - recursive: Whether the field is recursive.
    - tags: The list of tags of the field.
    '''
    Schema =  SchemaFieldClass(
                     fieldPath = fieldPath,
                     type = SchemaFieldDataTypeClass(type=selet_field_type(nativeDataType)),
                      nativeDataType = nativeDataType, 
                      jsonPath = "", 
                     nullable = nullable,
                     description = description,
                     recursive = recursive,  
                     globalTags = GlobalTagsClass(tags=get_tags(tags)),
                     glossaryTerms = GlossaryTermsClass(terms=[GlossaryTermAssociationClass(urn=make_term_urn("Classification.PII"))],
                     auditStamp = AuditStampClass(time=0,actor="urn:li:corpuser:ingestion")),
                     )
    
    return Schema




def create_dataset(database,table,fields,primaryKeys):
    '''
    This function creates the MetadataChangeProposalWrapper object.
    It takes the following parameters:
    - database: The database of the dataset.
    - table: The table of the dataset.
    - fields: The list of fields of the dataset.
    - platform: The platform of the dataset.
    - primaryKeys: The list of primary keys of the dataset.
    '''
    event = MetadataChangeProposalWrapper(
        entityUrn=make_dataset_urn(platform=database, name=table, env="PROD"),
        aspect=SchemaMetadataClass(
                                    schemaName="customer",  
                                    platform=make_data_platform_urn(database), 
                                    version=0, 
                                    hash="", 
                                    
                                    primaryKeys=primaryKeys,
                                    platformSchema=OtherSchemaClass(rawSchema="__insert raw schema here__"),
                                    fields=fields,
                                    )
        )
    return event




# This function saves the event to a json file
def save_to_json(json_path,event):
    json_file = post_json_transform(event.to_obj())
    json_object = json.dumps(json_file, indent=4)
    

    with open(os.path.join(json_path), "w") as outfile:
        outfile.write(json_object)


        


    