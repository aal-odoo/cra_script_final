# Script to import crm leads
import xlrd, logging
import sys, logging, errno
import datetime
import json

from tracemalloc import start
import csv
import datetime
from xmlrpc import client
import time

from xmlrpc import client

logging.getLogger().setLevel(logging.INFO)

# login information

# url2 = 'http://localhost:8069'
# db2 = 'cra1'
# username2 = 'admin'
# password2 = 'admin'

url = 'https://cracentral.odoo.com'
db = 'wisesolu-crapr-cracentral-984030'
username = 'api_user'
password = 'Apiuser123'

url2 = 'https://wisesolu-crafcgpr-staging-12454759.dev.odoo.com'
db2 = 'wisesolu-crafcgpr-staging-12454759'
username2 = 'admin2'
password2 = '5d7b5baca6c494d307189c67f9091ffb2df2d3e6'


TEST = True
BATCH = 200

END = datetime.datetime(2024, 3, 25, 23, 59, 59)

METRICSBATCH = 40000
OLDIDS = []
FIELDS = ['x_studio_contact','x_name', 'x_studio_measure_category','x_studio_measure_key','x_studio_last_service_code','x_studio_rendering_provider_npi','x_studio_appointment_date','x_studio_current_service_code',
          'x_studio_current_service_date','x_studio_official_compliance_date']


MODELFIELDS = {
    'x_insaludhcn': FIELDS + ['x_studio_notes','x_studio_diagnosis','x_studio_last_service_date_1','x_studio_rendering_provider','x_studio_transmission_date','x_studio_official_compliance_status','x_studio_internal_compliance_status','x_studio_estimated_month_closure','x_studio_hedis_value_set','x_studio_status'],
    'x_qualitymetrics': FIELDS + ['x_studio_quality_measure', 'x_studio_measure_description','x_studio_current_rate_cell', 'x_studio_current_rate_cell_description','x_studio_raf_score','x_studio_measure_status','x_studio_diagnosis_code','x_studio_diagnosis_description','x_studio_drug_name','x_studio_estimated_expiration_date','x_studio_last_service_date', 'x_studio_rendering_provider','x_studio_diagnosis_code','x_studio_transmission_date','x_studio_rescued_flag','x_studio_official_compliance_status_1','x_studio_official_expiration_date','x_studio_internal_compliance_status_1'],
    'x_rse_quality_measures': FIELDS + ['x_studio_quality_measure','x_studio_measure_description','x_studio_rate_cell_1','x_studio_ratecell_description','x_studio_raf_score','x_studio_measure_status','x_studio_diagnosis_code','x_studio_diagnosis_description','x_studio_drug_name','x_studio_estimated_expiration_date','x_studio_last_service_date', 'x_studio_rendering_provider_1','x_studio_diagnosis_code','x_studio_transmission_date_official','x_studio_rescued_flag','x_studio_current_hchn_clasification','x_studio_official_compliance_status','x_studio_official_expiration_date','x_studio_internal_compliance_status_1','x_studio_notes']
}

MODELS = {
    'x_insaludhcn': 'insalud',
    # 'x_rse_quality_measures': 'rse',
    # 'x_qualitymetrics': 'alianza',
    
    

}


class dbInfo(object):
    def __init__(self, url, db, username, password, uid=None, models=None):
        self.url = url
        self.db = db
        self.username = username
        self.password = password
        self.uid = uid
        self.models = models

def log_verification(db_info):
    common = client.ServerProxy('{}/xmlrpc/2/common'.format(db_info.url))
    print(common.version())
    uid = common.authenticate(db_info.db, db_info.username, db_info.password, {})

    models = client.ServerProxy('{}/xmlrpc/2/object'.format(db_info.url))
    access = models.execute_kw(db_info.db, uid, db_info.password,
        'res.partner', 'check_access_rights',
        ['write'], {'raise_exception': False})
    if not access:
        logging.warning('Current user does not have the correct access to {}'.format('res.partner'))

    db_info.uid = uid
    db_info.models = models
    return access




def process_batch(db_info,records):

   
    new_ids = db_info.models.execute_kw(db_info.db, db_info.uid, db_info.password,
            'quality.metrics', 'create', [records])

    print("Created Metrics: ",new_ids)

    logging.info('Sucessfully created batch of: ' + str(len(new_ids)))

    return new_ids

def get_partner_id(db_info, partner):
    if partner == False:
        return False
    partner = db_info.models.execute_kw(db_info.db, db_info.uid, db_info.password,
            'res.partner', 'search_read', [[('old_id','=',partner[0])]], {'fields': ['id']})
    if partner:
        return partner[0]['id']
    return False

def get_user(cache, val):
    if val == False:
        return False
    return cache['user'][str(val[0])]

def get_quality_rse_alianza_values(new_vals,old_vals):
    new_vals['quality_measure'] = old_vals['x_studio_quality_measure']
    new_vals['measure_description'] = old_vals['x_studio_measure_description']
    new_vals['raf_score'] = old_vals['x_studio_raf_score']
    new_vals['measure_status'] = old_vals['x_studio_measure_status']
    new_vals['diagnosis_code'] = old_vals['x_studio_diagnosis_code']
    new_vals['diagnosis_description'] = old_vals['x_studio_diagnosis_description']
    new_vals['drug_name'] = old_vals['x_studio_drug_name']
    new_vals['estimated_expiration_date'] = old_vals['x_studio_estimated_expiration_date']
    new_vals['last_service_date']= old_vals['x_studio_last_service_date']
    new_vals['current_diagnosis_code']= old_vals['x_studio_diagnosis_code']
    new_vals['rescued_flag']= old_vals['x_studio_rescued_flag']
    new_vals['official_expiration_date']= old_vals['x_studio_official_expiration_date']
    new_vals['internal_compliance_status']= old_vals['x_studio_internal_compliance_status_1']

    return new_vals

def get_insalud_values(new_vals,old_vals):
    new_vals['diagnosis_code'] = old_vals['x_studio_diagnosis']
    new_vals['last_service_date']= old_vals['x_studio_last_service_date_1']
    new_vals['render_provider']= old_vals['x_studio_rendering_provider']
    new_vals['transmission_date']= old_vals['x_studio_transmission_date']
    new_vals['official_compliance_status']= old_vals['x_studio_official_compliance_status']
    new_vals['insalud_internal_compliance_status']= old_vals['x_studio_internal_compliance_status']
    
    new_vals['estimated_month_closure']= old_vals['x_studio_estimated_month_closure']
    new_vals['hedis_value_set']= old_vals['x_studio_hedis_value_set']
    new_vals['status']= old_vals['x_studio_status']
    new_vals['notes'] =  old_vals['x_studio_notes']
    return new_vals

def get_quality_metrics_values(db_info,batch,model, cache):
    metrics = []
    for old_vals in batch:
        partner = get_partner_id(db_info, old_vals['x_studio_contact'])
        new_vals = {
            'quality_type': MODELS[model],
            'partner_id': partner,
            'old_id': old_vals['id'],
            'name': MODELS[model] + ' Quality Measures',

            'measure_category': old_vals['x_studio_measure_category'],
            'measure_key': old_vals['x_studio_measure_key'],
            'last_service_code': old_vals['x_studio_last_service_code'],

            'render_provider_npi':old_vals['x_studio_rendering_provider_npi'],
            'app_date': old_vals['x_studio_appointment_date'],

            'current_service_code': old_vals['x_studio_current_service_code'],
            'current_service_date': old_vals['x_studio_current_service_date'],

            'official_compliance_date': old_vals['x_studio_official_compliance_date'],

            # 'custom_write_uid': get_user(cache, old_vals['write_uid']),
            # 'custom_create_uid': get_user(cache, old_vals['create_uid']),
            # 'custom_create_date': old_vals['create_date'],
        }
        
        if model == "x_rse_quality_measures":
            new_vals = get_quality_rse_alianza_values(new_vals,old_vals)
            new_vals['current_rate_cell_desc']= old_vals['x_studio_ratecell_description']
            new_vals['current_rate_cell']= old_vals['x_studio_rate_cell_1']
            new_vals['render_provider']= old_vals['x_studio_rendering_provider_1']
            new_vals['transmission_date']= old_vals['x_studio_transmission_date_official']
            new_vals['current_hchn_classification']= old_vals['x_studio_current_hchn_clasification']
            new_vals['official_compliance_status']= old_vals['x_studio_official_compliance_status']
            new_vals['notes'] =  old_vals['x_studio_notes']

        elif model == 'x_qualitymetrics':
            new_vals = get_quality_rse_alianza_values(new_vals,old_vals)

            new_vals['current_rate_cell_desc']= old_vals['x_studio_current_rate_cell_description']
            new_vals['current_rate_cell']= old_vals['x_studio_current_rate_cell']
            new_vals['render_provider']= old_vals['x_studio_rendering_provider']
            new_vals['transmission_date']= old_vals['x_studio_transmission_date']
            new_vals['official_compliance_status']= old_vals['x_studio_official_compliance_status_1']

        elif model == 'x_insaludhcn':
            new_vals = get_insalud_values(new_vals,old_vals)


        metrics.append(new_vals)
    return metrics




def get_data_chunk(data, batch):
    # for i in range(0, len(data), batch):
    for i in range(0, len(data), batch):
        yield data[i:i + batch]


def remove_users(db_info,cache):
    user = db_info.models.execute_kw(db_info.db, db_info.uid, db_info.password,
            'res.users', 'search_read', [['|',('active','=', False), ('active','=', True)]], {'fields': ['id', 'partner_id']})
    cache['partner'] =  [str(x['partner_id'][0]) for x in user]
    return cache

def create_cache(db_info):

    cache = {}
    
    user = db_info.models.execute_kw(db_info.db, db_info.uid, db_info.password,
            'res.users', 'search_read', [['|',('active','=', False), ('active','=', True)]], {'fields': ['id', 'old_id']})
    cache['user'] = {c['old_id']: c['id'] for c in user}
    return cache

def get_quality_metrics_records_db(db_info,offset,model):
    # records = db_info.models.execute_kw(db_info.db, db_info.uid, db_info.password,
    #         'x_qualitymetrics', 'search_read', [[]], {'fields': FIELDS,  'offset': offset, 'limit': METRICSBATCH})
    
    # records = db_info.models.execute_kw(db_info.db, db_info.uid, db_info.password,
    #         'x_rse_quality_measures', 'search_read', [[]], {'fields': FIELDS,  'offset': offset, 'limit': METRICSBATCH})
    records = db_info.models.execute_kw(db_info.db, db_info.uid, db_info.password,
            model, 'search_read', [[['create_date','<',END]]], {'fields': MODELFIELDS[model],  'offset': offset, 'limit': METRICSBATCH})

    return records

def run():

    # old database
    db_info = dbInfo(url, db, username, password) 
    #new database
    db_info2 = dbInfo(url2, db2, username2, password2)
    # Log in verification and user id/models var
    if_access = log_verification(db_info) 
    if_access2 = log_verification(db_info2)
    cache = create_cache(db_info2)
    #iterate through each model
    for model in list(MODELS.keys()):
        print(model, 'model')
        for i in range(0,500000,METRICSBATCH):
            print("START BATCH: ",i)
            quality_metrics_records = get_quality_metrics_records_db(db_info,i,model)
    

            count = 0
            tot = len(list(get_data_chunk(quality_metrics_records, BATCH)))
            for batch in list(get_data_chunk(quality_metrics_records, BATCH)):
                count += 1
                print ("count: %d/%d" % (count, tot))
                start = time.time()
                print ("Start of total time: ", start)

                start2 = time.time()
                print ("Start of collecting: ", start2)
                metrics = get_quality_metrics_values(db_info2,batch, model,cache)
                end2 = time.time()
                print ("End of collecting time: %d (%d)" % (end2, (end2 - start2)))

                start3 = time.time()
                print ("Start of creation: ", start3)
                process_batch(db_info2, metrics)
                end3 = time.time()
                print ("End of creation: %d (%d)" % (end3, (end3 - start3)))
                print("Quality Metrics Created: ", i+count*BATCH)

                end = time.time()
                print ("End of total time: %d (%d)" % (end, (end - start)))
    
if __name__ == "__main__":
    run()


# script for alianza to export excel for id and x_studio_notes and a script to import the notes