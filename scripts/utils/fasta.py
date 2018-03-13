headers = {
    "default": ['strain_name', 'sample_name', 'collection_date', 'country', 'division', 'subdivision', 'accession', 'accession_url', 'authors', 'attribution_journal', 'attribution_title', 'attribution_url'],
    "zika": [
        'strain_name',
        'virus',
        'accession',
        'collection_date',
        'empty', # region
        'country',
        'division',
        'location',
        'empty', # genbank field
        'empty', # genome
        'authors',
        'accession_url',
        'publication_name',
        'journal',
        'attribution_url'
    ],
    "fluA": [
        'strain_name',
        'collection_date',
        'country',
        'region',
        'segment',
        'type',
        'ha_type',
        'na_type',
        'host_species',
        'accession'
    ],
}
