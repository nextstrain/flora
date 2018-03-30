"""
The headers are keyed off of the database name (i.e. --db ... from the command line)
"""

headers = {
    "default": [
                'accession',
                'authors',
                'attribution_journal',
                'locus',
                'empty',
                'attribution_url',
                'attribution_source',
                'strain_name',
                'empty',
                'attribution_title',
                'sequence_url',
                'pathogen',
                'collection_date',
                'country',
                'division',
                'host_species',
                'number_sequences',
                'region'],
    "zika": [
                'accession',
                'authors',
                'attribution_journal',
                'locus',
                'empty',
                'attribution_url',
                'attribution_source',
                'strain_name',
                'empty',
                'attribution_title',
                'sequence_url',
                'pathogen',
                'collection_date',
                'country',
                'division',
                'host_species',
                'number_sequences',
                'region'],
    "lassa": [
        'accession',
        'strain_name',
        'collection_date',
        'segment',
        'country',
        'region'
    ]
}
