# import math

from datahub.emitter.mce_builder import (
                                            make_dataset_urn,
                                            make_schema_field_urn
                                        )
from datahub.emitter.mcp import MetadataChangeProposalWrapper
# from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.com.linkedin.pegasus2avro.dataset import (
                                                                DatasetLineageType,
                                                                FineGrainedLineage,
                                                                FineGrainedLineageDownstreamType,
                                                                FineGrainedLineageUpstreamType,
                                                                Upstream,
                                                                UpstreamLineage,
                                                                )


# This function creates a UpstreamLineage aspect for a dataset.
def field_lineages(df, downstream_database, downstream_table):
    df_tmp = df.T
    df_tmp.columns = df_tmp.loc['field_path']
    df_tmp = df_tmp.loc[[_ for _ in df.columns.to_list() if 'lineage' in _]]
    all_columns = df_tmp.columns.to_list()

    # Create a UpstreamLineage aspect for the dataset
    filed_lineage = filed_lineage_object(df_tmp, all_columns, downstream_database, downstream_table)

    upstream_dataset_urns = filed_lineage[0]
    fine_grained_lineages = filed_lineage[1]

    # Create a UpstreamLineage aspect for the dataset
    field_lineages = UpstreamLineage(
        upstreams=upstream_dataset_urns,
        fineGrainedLineages=fine_grained_lineages
    )

    # Create a MetadataChangeProposal for the dataset
    downstream_dataset = create_urn(database=downstream_database, table=downstream_table)
    lineage_mcp = MetadataChangeProposalWrapper(
        entityUrn=downstream_dataset,
        aspect=field_lineages
    )

    print(f'連結欄位{downstream_database} {all_columns}')
    return lineage_mcp

def create_urn(database, table, field=None):
    '''Create a URN for a dataset or a field
    Args:
        database (str): Database name
        table (str): Table name
        field (str): Field name
    Returns:
        urn (str): URN for a dataset or a field
            '''
    if field is None:
        return make_dataset_urn(database, table)
    else:
        return make_schema_field_urn(make_dataset_urn(database, table), field)

def filed_lineage_object(df_tmp, all_columns, downstream_database, downstream_table):
    '''Create a UpstreamLineage aspect for a dataset
    Args:
        df_tmp (DataFrame): DataFrame of the dataset
        all_columns (list): List of all columns
        downstream_database (str): Database name
        downstream_table (str): Table name
    Returns:
        upstream_dataset_urns (list): List of upstream dataset URNs
        fine_grained_lineages (list): List of fine-grained lineages'''
    upstream_dataset_urns = []
    fine_grained_lineages = []
    for upstreams in all_columns:
        # Check if the column has more than one lineage item
        check_lineage_items = [x for x in list(df_tmp[upstreams]) if len(x) > 1]

        if len(check_lineage_items) > 0:
            symbol = "@"
            item_lineage_urn = []
            for check_lineage_item in check_lineage_items:
                database = check_lineage_item.split(symbol)[0]
                table = check_lineage_item.split(symbol)[1]
                lineage_item = check_lineage_item.split(symbol)[2]
                # Create a URN for a field
                urn = create_urn(database=database, table=table, field=lineage_item)
                item_lineage_urn.append(urn)
            # Create a fine-grained lineage
            downstream_item = create_urn(database=downstream_database, table=downstream_table, field=upstreams)

            fine_grained_lineages.append(
                # The upstream and downstream types are set to FIELD_SET and FIELD
                FineGrainedLineage(
                    upstreamType=FineGrainedLineageUpstreamType.FIELD_SET,
                    upstreams=item_lineage_urn,
                    downstreamType=FineGrainedLineageDownstreamType.FIELD,
                    downstreams=[downstream_item]
                ))

            upstream_dataset_urn = create_urn(database=database, table=table)
            upstream_dataset_urns.append(
                Upstream(dataset=upstream_dataset_urn, type=DatasetLineageType.TRANSFORMED))

    return upstream_dataset_urns, fine_grained_lineages
