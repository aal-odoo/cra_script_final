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

url = 'http://localhost:8069'
db = 'cra1'
username = 'admin'
password = 'admin'

# url = 'https://wisesolu-crafcgpr-staging-12454759.dev.odoo.com'
# db = 'wisesolu-crafcgpr-staging-12454759'
# username = 'admin2'
# password = '5d7b5baca6c494d307189c67f9091ffb2df2d3e6'

TEST = True
BATCH = 200


class dbInfo(object):
    def __init__(self, url, db, username, password, uid=None, models=None):
        self.url = url
        self.db = db
        self.username = username
        self.password = password
        self.uid = uid
        self.models = models


# Logging Verification, returns uid and models
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


def get_xls(filepath):
    logging.info('Importing: {} '.format(filepath))
    workbook = xlrd.open_workbook(filepath)
    sheet = workbook.sheet_by_index(0)
    return sheet, workbook



# check if the state in the sheet is in db
def calc_state_id(state, country, cache):
    cur_country = cache['country_map'].get(country,False)
    if cur_country:
        cur_state = cur_country['states'].get(state,False)
        if cur_state:
            return cur_state['id']
    return ''

def get_formatted_dt(date, time=False):

    if date == '':
        return False
    if time:
        return datetime.datetime.strptime(date, '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d %H:%M:%S')
    newdate = datetime.datetime.strptime(date, '%Y-%m-%d').strftime('%Y-%m-%d')
    return newdate
# def convert_to_date(value,):
#     y, m , d, h, i, s = xlrd.xldate_as_tuple(value)
#     # return f"{m}-{d}-{y}"
#     return f"{y}-{m}-{d}"

def create_missing_ids(db_info, values, cache):
    values = list(set([x for x in values if x.strip() and x not in cache['employee_ids']]))
    new_employees = []
    for val in values:
        new_employees.append({
            'name': str(val)
        })
    new_ids = db_info.models.execute_kw(db_info.db, db_info.uid, db_info.password,
            'hr.employee','create',[new_employees])

    new_dict = {k:v for k,v in zip(values,new_ids)}
    cache['employee_ids'].update(new_dict)

    return new_dict

def create_cache(db_info):
    cache = {}

    state_list = db_info.models.execute_kw(db_info.db, db_info.uid, db_info.password,
            'res.country.state', 'search_read',[],
            {'fields': ['id', 'name', 'code', 'country_id']})
    cache['state_codes'] = {x['code']:(x['name'],x['country_id']) for x in state_list}
    cache['state_ids'] = {x['name']:x['id'] for x in state_list}

    country_list = db_info.models.execute_kw(db_info.db, db_info.uid, db_info.password,
            'res.country', 'search_read',[],
            {'fields': ['id', 'name', 'state_ids']})
    cache['country_ids'] = {x['name']:x['id'] for x in country_list}

    cache['country_map'] = {c['name']: {'id': c['id'], 'states':{s['code']: {'name':s['name'],'id':s['id']} for s in state_list if s['country_id'][0] == c['id']}} for c in country_list }

    user_list = db_info.models.execute_kw(db_info.db, db_info.uid, db_info.password,
            'res.users', 'search_read',[['|',('active','=',True),('active','=',False)]],
            {'fields': ['id', 'old_id','old_partner_id']})
    cache['users'] = {str(x['old_id']):x['id'] for x in user_list}
    cache['partner'] = [x['old_partner_id'] for x in user_list]
    
    pcp_records = db_info.models.execute_kw(db_info.db, db_info.uid, db_info.password,
            'pcp', 'search_read', [[]], {'fields': ['id', 'pcp_npi',]})
    cache['pcp'] = {p['pcp_npi']: p['id'] for p in pcp_records}

    pmg_records = db_info.models.execute_kw(db_info.db, db_info.uid, db_info.password,
            'pmg', 'search_read', [[]], {'fields': ['id', 'old_id',]})
    cache['pmg'] = {str(p['old_id']): p['id'] for p in pmg_records}
    company_records = db_info.models.execute_kw(db_info.db, db_info.uid, db_info.password,
            'res.company', 'search_read', [[]], {'fields': ['id', 'old_id',]})
    cache['company'] = {c['old_id']: c['id'] for c in company_records}



    return cache


def format_value(field_name, row, cache, use_cache=False):
    
    return row.get(str(field_name), '')

def get_state_and_country(row, cache,cache2):
    state_id = row.get('state_id','')
    if state_id == '':
        return False, False
    state_dict = cache2[state_id]
    state = cache['state_ids'][state_dict['name']]
    country = cache['country_ids'][state_dict['country']]
    return state,country

def remove_decimal(value):
    if value == '':
        return value
    else:
        res = value.split('.')
        return res[0]


def get_cache_id(field, cache_name, row,cache):
    record = row.get(field, False)
    record_id = cache[cache_name][remove_decimal(str(record))] if not record == '' else False
    return record_id

def create_pcp(db_info,cache,name,npi,company):
    if len(name) == 0:
        return False
    
    if cache['pcp'].get(npi,False):
        return cache['pcp'][npi]
    

    vals = {'name':name, 'company_id':company, 'pcp_npi':npi}


    new_id = db_info.models.execute_kw(db_info.db, db_info.uid, db_info.password,
            'pcp', 'create', [[vals]])
    cache['pcp'][npi] = new_id[0]
    return new_id[0]

def create_contact(to_create, cache, db_info, cache2):
    # Format the contacts for create
    # For each row in contacts to create
    create_list = []

    for row in to_create:
        
        
        # Handle the OOB odoo fields first
        old_id = str(int(format_value('id', row, cache, )))
        if old_id in cache['partner']:
            continue
        age = format_value('age', row, cache, )
        date = format_value('x_studio_fecha_de_nacimiento_1', row, cache, )
        birth_date = format_value('birth_date', row, cache, )
        state,country = get_state_and_country(row, cache, cache2)
        create_date = format_value('create_date', row, cache, ).split('.')[0]
        write_date = format_value('custom_write_date', row, cache, ).split('.')[0]
        company_id = get_cache_id('company_id','company',row,cache)
        pcp_name = format_value('pcp_name', row, cache, )
        if len(pcp_name)==0:
            pcp_name = format_value('x_studio_pcp_name', row, cache, )
        pcp_npi = remove_decimal(str(format_value('pcp_npi', row, cache, )))
        if len(pcp_npi) ==0:
            pcp_npi = remove_decimal(str(format_value('x_studio_pcp_npi', row, cache, )))
        contact = {
            'old_id': old_id,
            'active': True if row.get('active') == 't' else False,
            'name': format_value('name', row, cache, ),
            'type': 'contact',
            'is_company': True if row.get('is_company') == 't' else False,
            'insurance_company': format_value('insurance_company',row,cache),
            'spmg_name': format_value('spmg_name',row,cache),
            'spmg_npi': format_value('spmg_npi',row,cache),
            'pmg_id': get_cache_id('pmg_id','pmg',row,cache),
            'pmg_npi': format_value('pmg_npi',row,cache),
            'pcp_id': create_pcp(db_info,cache,pcp_name,pcp_npi,company_id),
            'is_pcp': True if row.get('x_studio_is_a_pcp') == 't' else False,
            'speciality_code': format_value('specialty_code',row,cache),
            'speciality_description': format_value('specialty_code_description',row,cache),
            'assigned_center': format_value('x_studio_assigned_center',row,cache),
            'street': format_value('street',row,cache),
            'street2':format_value('street2',row,cache),
            'city': format_value('city',row,cache),
            'zip': remove_decimal(str(format_value('zip', row, cache, ))),
            'state_id':state,
            'country_id':country,

            'company_id':company_id,


            'member_id': format_value('member_id',row,cache),
            'prev_member_id': format_value('x_studio_previous_member_id',row,cache),
            'membership_status': format_value('membership_status',row,cache),
            'age': 0 if age=='' else int(age), 
            'hchn': format_value('hchn',row,cache), 
            'sex': format_value('sex',row,cache),
            'birth_date': get_formatted_dt(birth_date),
            'phone': remove_decimal(str(format_value('phone', row, cache, ))),
            'mobile': remove_decimal(str(format_value('mobile', row, cache, ))),
            'phone_extension': remove_decimal(str(format_value('x_studio_phone_ext',row,cache))),
            'alternate_phone2': remove_decimal(str(format_value('x_studio_phone_alternate_2',row,cache))),
            'email': format_value('email',row,cache),
            'region': format_value('region',row,cache), 
            'ases_prem': format_value('ases_premium',row,cache),
            'paymonth': format_value('pay_month',row,cache),
            'benefit_plan': format_value('x_studio_benefit_plan',row,cache),
            'benefit_plan_desc': format_value('x_studio_benefit_plan_description',row,cache), 
            'tier': format_value('x_studio_tier',row,cache),
            'ratecell': format_value('rate_cell',row,cache),
            'ratecell_description': format_value('rate_cell_description',row,cache),


            'ipa_category': format_value('x_studio_ipa_category',row,cache),
            'organization_id': format_value('x_studio_organization_id',row,cache),
            'organization': format_value('x_studio_organization_1',row,cache),
            'organization_npi': format_value('x_studio_organization_npi',row,cache),
            'billing_provider_name': format_value('x_studio_billing_provider_name',row,cache),
            'billing_provider_npi': format_value('x_studio_billing_provider_npi',row,cache),
            'membership_type': format_value('membership_type',row,cache),
            
            'coverage': format_value('x_studio_coverage',row,cache),
            'raf': format_value('x_studio_raf',row,cache),
            'total_revenue': format_value('x_studio_total_revenue',row,cache),
            'cms_premium': format_value('x_studio_cms_premium',row,cache),
            'plantino_flag': format_value('x_studio_platino_flag',row,cache),
            'hospice_flag': format_value('x_studio_hospice_flag',row,cache),
            'esrd_flag': format_value('x_studio_esrd_flag',row,cache),
            'cms_category': format_value('x_studio_cms_category',row,cache),
            'membership_key': format_value('x_studio_member_key',row,cache),
            
            'custom_create_date': get_formatted_dt(create_date,True),
            'custom_create_uid': get_cache_id('create_uid','users',row,cache),
            'custom_write_date': get_formatted_dt(write_date,True),
            'custom_write_uid': get_cache_id('custom_write_uid','users',row,cache),

        }
        

        create_list.append(contact)
    return process_batch(create_list, db_info)


def process_batch(create_list, db_info):
   
    new_ids = db_info.models.execute_kw(db_info.db, db_info.uid, db_info.password,
            'res.partner', 'create', [create_list],{'context': {'no_update': True}})

    print("Created tickets: ",new_ids)

    logging.info('Sucessfully created batch of: ' + str(len(new_ids)))

    return new_ids

def create_cache_state_country(data2):
    cache2 = {}
    idx = 0
    for city in data2:
        if idx == 0:
            idx +=1
            continue
        idx+=1
        
        city_id = city[0]
        city_name = city[1]
        country = city[2]
        if city_name =="PR":
            city_name = "Puerto Rico"
            country = "United States"
        
        cache2[city_id] = {'name': city_name,'country': country}
    return cache2

def process_file(reader):
    data = []
    idx = 0
    headers = False
    for row in reader:
        if idx == 0:
            headers = row
        else:   
            items = dict(zip(headers, row))
            data.append(items)
        idx+=1
    return data

def get_data_chunk(data, batch):
    for i in range(0, len(data), batch):
        yield data[i:i + batch]

def run():

    db_info = dbInfo(url, db, username, password)
    # Log in verification and user id/models var
    if_access = log_verification(db_info)
    link = ['cra_partner_for_cra.csv','cra_partner_for_insalud.csv','cra_partner_for_none.csv','cra_partner_for_rse.csv','cra_partner_for_alianza.csv']
    # path = 'cra_partner_for_cra.csv'
    # path = 'cra_partner_for_insalud.csv'
    # path = 'cra_partner_for_none.csv'
    # path = 'cra_partner_for_rse.csv'
    # path = 'cra_partner_for_alianza.csv'
    
    
    
    
    for path in link:
        with open(path, newline="") as csvfile:
            reader = csv.reader(csvfile, delimiter=",")
            data = process_file(reader)
            
            path2 = 'city_country_records.csv'
            with open(path2, newline="") as csvfile2:
                reader2 = csv.reader(csvfile2, delimiter=",")
                data2 = create_cache_state_country(reader2)

                cache = create_cache(db_info)
                count = 0
                tot = len(list(get_data_chunk(data, BATCH)))
                for d in list(get_data_chunk(data, BATCH)):
                    count += 1
                    print ("count: %d/%d" % (count, tot))
                    db_ids = create_contact(d, cache, db_info, data2)


if __name__ == "__main__":
    run()
