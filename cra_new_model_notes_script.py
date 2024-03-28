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


TEST = True
BATCH = 500
SKIP = True

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

def format_value(field_name, row):
    
    return row.get(str(field_name), '')

def create_contact(to_create, db_info):
    # Format the contacts for create
    # For each row in contacts to create
    override_list = []

    for row in to_create:
        old_id = str(int(format_value('id', row, )))

        curr_id = db_info.models.execute_kw(db_info.db, db_info.uid, db_info.password,
            'quality.metrics', 'search', [[['old_id', '=', old_id],['quality_type','=','alianza']]], {'limit': 1})
        # Handle the OOB odoo fields first
        notes = format_value('x_studio_notes', row, )

        if curr_id:
            try:
                db_info.models.execute_kw(db_info.db, db_info.uid, db_info.password,
                    'quality.metrics', 'write', [curr_id,{'notes':notes}],)
               
            except:
                notes = notes.replace('','')
                db_info.models.execute_kw(db_info.db, db_info.uid, db_info.password,
                    'quality.metrics', 'write', [curr_id,{'notes':notes}],)
            override_list.append(curr_id)
    print("Override tickets: ",override_list)

    logging.info('Sucessfully created batch of: ' + str(len(override_list)))
    return True


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
    # path = 'cra_alianza_rec.csv'
    path = 'cra_new_model_check.csv'

    with open(path, newline="") as csvfile:
        reader = csv.reader(csvfile, delimiter=",")
        data = process_file(reader)
        

        count = 0
        tot = len(list(get_data_chunk(data, BATCH)))
        for d in list(get_data_chunk(data, BATCH)):
            count += 1
            print ("count: %d/%d" % (count, tot))
            
            db_ids = create_contact(d, db_info,)


if __name__ == "__main__":
    run()
