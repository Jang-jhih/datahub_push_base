import pandas as pd
import os
import json
import math
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.emitter.mce_builder import (
                                            make_data_platform_urn,
                                            make_dataset_urn,
                                            make_tag_urn,
                                            make_term_urn,
                                        )

from datahub.emitter.serialization_helper import (
                                                    pre_json_transform,
                                                    post_json_transform
                                                  )

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




def select_field_type(native_data_type):
    '''Select field type
    Args:
        native_data_type (str): Native data type
    Returns:
        type_dict[native_data_type] (str): Field type
            '''
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
    
    if native_data_type in type_dict:
        return type_dict[native_data_type]
    else:
        raise Exception("Type not found")


def get_tags(tags):
    '''Get tags for a field
    Args:
        tags (str): Tags
    Returns:
        tag_association (list): List of tag associations'''
    if not tags:
        return None
    else:
        tags = tags.split(',')
        tag_association = []
        for tag in tags:
            tag_urn = make_tag_urn(tag)
            if not tag_urn:
                return None
            #TagAssociationClass is urn of a tag
            tag_association.append(TagAssociationClass(tag=tag_urn))
        return tag_association

def create_field_object(field_path, native_data_type, nullable, recursive, tags, description):
    '''Create a field object for ingestion into DataHub
    Args:
        field_path (str): Acctually, this is the field name
        native_data_type (str): Native data type
        nullable (bool): Nullable" tag is used to indicate whether a data field allows the existence of Null values
        recursive (bool): Recursive
        tags (str): "tags" is used to label specific tags for this field
        description (str): "description" is used to describe the relevant description of the field.
    Returns:
        schema (SchemaFieldClass): Field object
            '''
    if len(tags) < 1:
        global_tags = None
    else:
        #GlobalTagsClass is a list of TagAssociationClass,which consists of URNs representing tags.
        global_tags = GlobalTagsClass(tags=get_tags(tags))
    
    if field_path is None or native_data_type is None:
        raise Exception("Field path or native data type are not defined")
    
    if select_field_type(native_data_type) is None:
        raise Exception("Native data type is not supported")
    
    schema = SchemaFieldClass(
        fieldPath=field_path,
        type=SchemaFieldDataTypeClass(type=select_field_type(native_data_type)),
        nativeDataType=native_data_type,
        jsonPath="",
        nullable=nullable,
        description=description,
        recursive=recursive,
        globalTags=global_tags,
    )
    
    return schema



def create_dataset_object(database, table, fields, primary_keys):
    '''Create a dataset object for ingestion into DataHub
    Args:
        database (str): Database name
        table (str): Table name
        fields (list): List of fields
        primary_keys (list): List of primary keys
    Returns:
        event (MetadataChangeProposalWrapper): Dataset object
            '''
    try:
        event = MetadataChangeProposalWrapper(
            entityUrn=make_dataset_urn(platform=database, name=table, env="PROD"),
            aspect=SchemaMetadataClass(
                schemaName="customer",
                platform=make_data_platform_urn(database),
                version=0,
                hash="",
                primaryKeys=primary_keys,
                platformSchema=OtherSchemaClass(rawSchema="__insert raw schema here__"),
                fields=fields,
            )
        )
        return event
    except Exception as e:
        print(e)


def drop_null(list):
    '''Drop null values from a list
    Args:
        list (list): List of values
    Returns:
        list (list): List of values without null values
            '''
    if not isinstance(list, pd.Series):
        return "Input data is not a pandas series"
    try:
        return [_ for _ in list if pd.notnull(_)]
    except TypeError:
        return "Input data is not a list of numbers"





    