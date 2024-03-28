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

# url = 'http://localhost:8069'
# db = 'cra1'
# username = 'admin'
# password = 'admin'

url = 'https://wisesolu-crafcgpr-staging-12454759.dev.odoo.com'
db = 'wisesolu-crafcgpr-staging-12454759'
username = 'admin2'
password = '5d7b5baca6c494d307189c67f9091ffb2df2d3e6'

TEST = True
BATCH = 500


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



def create_cache(db_info):
    cache = {}

    company_records = db_info.models.execute_kw(db_info.db, db_info.uid, db_info.password,
            'res.company', 'search_read', [[]], {'fields': ['id', 'old_id',]})
    cache['company'] = {c['old_id']: c['id'] for c in company_records}



    return cache


def format_value(field_name, row, cache, use_cache=False):
    
    return row.get(str(field_name), '')


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

def create_contact(to_create, cache, db_info):
    # Format the contacts for create
    # For each row in contacts to create
    create_list = []

    for row in to_create:
        
        
        # Handle the OOB odoo fields first
        
        company_id = get_cache_id('company_id','company',row,cache)
        contact = {
            'old_id': str(int(format_value('id', row, cache, ))),
            'name': format_value('name', row, cache, ),
            'company_id':company_id,


            

        }
        

        create_list.append(contact)
    return process_batch(create_list, db_info)


def process_batch(create_list, db_info):
   
    new_ids = db_info.models.execute_kw(db_info.db, db_info.uid, db_info.password,
            'pmg', 'create', [create_list],{'context': {'no_update': True}})

    print("Created tickets: ",new_ids)

    logging.info('Sucessfully created batch of: ' + str(len(new_ids)))

    return new_ids


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

    path = 'cra_pmg.csv'

    with open(path, newline="") as csvfile:
        reader = csv.reader(csvfile, delimiter=",")
        data = process_file(reader)
        

        cache = create_cache(db_info)
        count = 0
        tot = len(list(get_data_chunk(data, BATCH)))
        for d in list(get_data_chunk(data, BATCH)):
            count += 1
            print ("count: %d/%d" % (count, tot))
            db_ids = create_contact(d, cache, db_info)


if __name__ == "__main__":
    run()
