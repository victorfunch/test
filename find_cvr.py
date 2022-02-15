import pandas as pd
from elasticsearch import Elasticsearch, RequestsHttpConnection

def create_es_connection():
    es = Elasticsearch(['http://distribution.virk.dk/cvr-permanent'], port = 80, 
    http_auth = ('hbseconomics.dk_CVR_I_SKYEN', '3e8841f0-67ff-4030-ab59-c4975527ac73'),
    connection_class = RequestsHttpConnection,
    use_ssl = False,
    verify_cert = False,
    timeout = 100)
    return es

date = '2021-05-21'
brancher = ['062000','091000','522220','061000']

return_from_es = []
for branche in brancher:
    body = {"query": {"bool": {"must": [{"match":{"Vrvirksomhed.hovedbranche.branchekode":branche}}]}}}
    cvr_search = create_es_connection().search(size=3000, explain=True, body=body, from_=0)
    return_from_es.append(cvr_search)

list_of_dfs = []

for branche_idx, branche in zip(range(0, len(return_from_es[0])), brancher):
    branche_return = return_from_es[branche_idx]['hits']['hits']
    
    virk_cvr = []
    virk_navn = []
    virk_form = []
    virk_branche = []
    virk_aarsvaerk = []

    for virk_idx in range(0, len(branche_return)):
        virk_return = branche_return[virk_idx]['_source']['Vrvirksomhed']
        virk_cvr.append(virk_return['cvrNummer'])
        virk_navn.append(virk_return['virksomhedMetadata']['nyesteNavn']['navn'])
        virk_form.append(virk_return['virksomhedMetadata']['nyesteVirksomhedsform']['langBeskrivelse'])
        virk_branche.append(branche)
        try:
            virk_aarsvaerk.append(virk_return['virksomhedMetadata']['nyesteAarsbeskaeftigelse']['antalAarsvaerk'])
        except:
            virk_aarsvaerk.append('Ikke tilgængelig')


    list_of_dfs.append(pd.DataFrame({
            'cvr' : virk_cvr
            , 'navn' : virk_navn
            , 'virkform' : virk_form
            , 'branche' : virk_branche
            , 'aarsvaerk' : virk_aarsvaerk
        }))

branche_df = pd.concat(list_of_dfs, ignore_index=True)
branche_df.to_excel(r'H:\HBS Drift\Medarbejdere\Victor\branche_matilde\cvr_numre.xlsx', index=False)