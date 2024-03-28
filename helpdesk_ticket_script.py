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
# db = 'aal-odoo-dummy-prod-7210025'
# username = 'admin'
# password = 'admin'

url = 'http://localhost:8069'
db = 'cra1'
username = 'admin'
password = 'admin'


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
        'helpdesk.ticket', 'check_access_rights',
        ['write'], {'raise_exception': False})
    if not access:
        logging.warning('Current user does not have the correct access to {}'.format('helpdesk.ticket'))

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

    if date == '' or date == False:
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


    user_list = db_info.models.execute_kw(db_info.db, db_info.uid, db_info.password,
            'res.users', 'search_read',[['|',('active','=',True),('active','=',False)]],
            {'fields': ['id', 'old_id','name']})
    cache['users'] = {str(x['old_id']):x['id'] for x in user_list}
    cache['nurse'] = {str(x['name']):x['id'] for x in user_list}

    # pcp_list = db_info.models.execute_kw(db_info.db, db_info.uid, db_info.password,
    #         'pcp', 'search_read',[],
    #         {'fields': ['id', 'old_id']})
    # cache['pcp'] = {str(x['old_id']):x['id'] for x in pcp_list}

    # partner_list = db_info.models.execute_kw(db_info.db, db_info.uid, db_info.password,
    #         'res.partner', 'search_read',[['|',('active','=',True),('active','=',False)]],
    #         {'fields': ['id', 'old_id']})
    # cache['partner'] = {str(x['old_id']):x['id'] for x in partner_list}
    helpdesk_team = db_info.models.execute_kw(db_info.db, db_info.uid, db_info.password,
            'helpdesk.team', 'search_read',[],
            {'fields': ['id', 'old_id']})
    cache['team'] = {str(x['old_id']):x['id'] for x in helpdesk_team}

    helpdesk_type = db_info.models.execute_kw(db_info.db, db_info.uid, db_info.password,
            'helpdesk.ticket.type', 'search_read',[],
            {'fields': ['id', 'old_id']})
    cache['helpdesk_type'] = {str(x['old_id']):x['id'] for x in helpdesk_type}

    stage = db_info.models.execute_kw(db_info.db, db_info.uid, db_info.password,
            'helpdesk.stage', 'search_read',[],
            {'fields': ['id', 'old_id']})
    cache['stage'] = {str(x['old_id']):x['id'] for x in stage}
    company = db_info.models.execute_kw(db_info.db, db_info.uid, db_info.password,
            'res.company', 'search_read',[],
            {'fields': ['id', 'old_id']})
    cache['company'] = {str(x['old_id']):x['id'] for x in company}
    category = db_info.models.execute_kw(db_info.db, db_info.uid, db_info.password,
            'helpdesk.category', 'search_read',[],
            {'fields': ['id', 'old_id']})
    cache['category'] = {str(x['old_id']):x['id'] for x in category}

    room = db_info.models.execute_kw(db_info.db, db_info.uid, db_info.password,
            'emergency.room', 'search_read',[],
            {'fields': ['id', 'name']})
    cache['room'] = {x['name']:x['id'] for x in room}
    return cache


def format_value(field_name, row, cache, selection=False):
    val = row.get(field_name, '')
    
    if val == '':
        return False
    return val


def remove_decimal(value):
    if value == '':
        return value
    else:
        res = value.split('.')
        return res[0]

def get_record(record,model,db_info):
    if record == False:
        return False
    ids = db_info.models.execute_kw(
        db_info.db, db_info.uid, db_info.password, model, "search", [[['name', "=", record]]]
    )
    if len(ids) == 0:
        vals = {
            'name': record
        }
        record_id = db_info.models.execute_kw(
            db_info.db, db_info.uid, db_info.password, model, "create", [vals]
            )
        return record_id
    
    return ids[0]



def get_cache_id(field, cache_name, row,cache):
        
    record = row.get(field, False)
    record_id = cache[cache_name][remove_decimal(str(record))] if not record == '' else False
    
    return record_id

def get_emergency_room(db_info, cache,room):
    if not room:
        return False
    if cache['room'].get(room,False):
        return cache['room'].get(room,False)
    vals = {
            'name': room
        }
    record_id = db_info.models.execute_kw(
        db_info.db, db_info.uid, db_info.password, 'emergency.room', "create", [vals]
    )
    cache['room'][room] = record_id
    return record_id

def get_nurse(cache, nurse):
    if not nurse:
        return False
    if nurse == 'Ana G. Ramos Vázquez':
        nurse = 'Ana G. Ramos'
    elif nurse == 'Karla Rosado':
        nurse = 'Karla Rosado Marrero'
    elif nurse == 'Milagros De León':
        nurse = 'Milagros De Leon'
    elif nurse == 'Aidaliz Velázquez':
        nurse = 'Aidaliz Velazquez'
    elif nurse == 'Ana González':
        nurse = 'Ana Gonzalez'


    return cache['nurse'][nurse]

def create_contact(to_create, cache, db_info):
    # Format the contacts for create
    # For each row in contacts to create
    create_list = []
    start = time.time()
    print ("Start setting up: ", start)
    
    for row in to_create:
        
        # external_list.append(int(row.get('Company ID')) if row.get('Company ID') else '')
        
        # Handle the OOB odoo fields first
        # pcp = format_value('x_studio_pcp_name', row, cache,use_cache=True)
        partner = int(format_value('partner_id', row, cache, ))
        partner_rec = db_info.models.execute_kw(db_info.db, db_info.uid, db_info.password,
            'res.partner', 'search',[[('old_id','=',partner),'|',('active','=',True),('active','=',False)]])
        
        start_call = format_value('x_studio_da_y_hora_de_la_llamada', row, cache, )
        coordinate = format_value('x_studio_fecha_y_hora_cita_coordinada', row, cache, )
        birth_date = format_value('x_studio_birth_date', row, cache, )
        expiration = format_value('x_studio_medicaid_expiration_date', row, cache, )
        pcp_last_visit = format_value('x_studio_pcp_last_visit_1', row, cache, )
        call_ended = format_value('x_studio_dia_y_hora_finalizada_la_llamada', row, cache, )
        create_date = format_value('create_date', row, cache, ).split('.')[0]
        write_date = format_value('custom_write_date', row, cache, ).split('.')[0] if format_value('custom_write_date', row, cache, ) else False
        contact = {
            'old_id': str(int(format_value('id', row, cache, ))),
            'name': format_value('name', row, cache, ),
            'partner_id': partner_rec[0],
            'priority': format_value('priority', row, cache, ),
            'team_id': get_cache_id('team_id','team', row, cache, ), #search by external id
            'ticket_type_id':get_cache_id('ticket_type_id','helpdesk_type',row,cache) ,
            'user_id': get_cache_id('user_id','users',row, cache),
            'partner_phone': format_value('phone_lost', row, cache, ),
            'partner_email': format_value('partner_email', row, cache, ),
            'email_cc': format_value('email_cc', row, cache, ),
            'kanban_state': format_value('kanban_state', row, cache, ),
            'company_id': get_cache_id('company_id','company',row, cache),
            'category': get_cache_id('x_studio_field_qKr2V','category',row, cache),
            'stage_id': get_cache_id('stage_id','stage',row, cache),
            'description': format_value('description', row, cache, ),

            #header
            'phone_number': format_value('x_studio_phone_number', row, cache, ),
            'alternate_phone': format_value('x_studio_alternate_phone', row, cache, ),
            'alternate_phone2': format_value('x_studio_alternate_2_phone', row, cache, ),
            'member_id': format_value('x_studio_member_id', row, cache, ),
            'insurance_company': format_value('x_studio_insurance_company_1', row, cache, ),
            'competition': format_value('x_studio_competition', row, cache, ),
            'coverage': format_value('x_studio_coverage_1', row, cache, ),
            'hchn': format_value('x_studio_hchn', row, cache, ),
            'ratecell_description': format_value('x_studio_rate_cell_description', row, cache, ),
            'birth_date': get_formatted_dt(birth_date),
            'age': format_value('x_studio_age', row, cache, ),
            'sex': format_value('x_studio_sex', row, cache, ),
            'lost_call_phone': format_value('phone_lost', row, cache, ),
            'visit_flag': format_value('x_studio_visit_flag', row, cache, ),
            'pmg_name': format_value('x_studio_pmg_name_official_1', row, cache, ),
            'pcp_name': format_value('x_studio_pcp_name_1', row, cache, ),
            'medicaid_expiration': get_formatted_dt(expiration),

            'date_time_coordinated': get_formatted_dt(coordinate,True),
            'aha_completed': format_value('x_studio_aha_completed', row, cache, ),
            'customer_satisfaction': format_value('x_studio_nivel_de_satisfaccion_del_cliente_1', row, cache, ),
            'vaccination_status': format_value('x_studio_vaccination_status_1', row, cache, ),
            'covid_19_vaccine': format_value('x_studio_vaccine_covid_19', row, cache, ),
            'preferred_location': format_value('x_studio_preferred_location_1', row, cache, ),
            'partner_email': format_value('partner_email', row, cache, ),


            'custom_create_date': get_formatted_dt(create_date,True),
            'custom_create_uid': get_cache_id('create_uid','users',row,cache),
            'custom_write_date': get_formatted_dt(write_date,True),
            'custom_write_uid': get_cache_id('custom_write_uid','users',row,cache),
            'stage_id': get_cache_id('stage_id', 'stage', row,cache),
            
        # teleconsultas
            #paso1
            'patient_phone': format_value('x_studio_patient_phone_number', row, cache, ),
            'start_call_datetime': get_formatted_dt(start_call,True),
            'reason': format_value('x_studio_razon_de_la_llamada', row, cache,True ),
            'call_911': format_value('x_studio_se_llam_al_911_1', row, cache,True ),
            'current_location': format_value('x_studio_dnde_se_encuentra_usted_ahora_mismo', row, cache, ),
            'accompanied': format_value('x_studio_est_usted_acompaado', row, cache,True ),
            'companion_drivers': format_value('x_studio_acompaante_conduce_y_pudede_llevarle_a_er', row, cache,True ),
            'present_symptoms': format_value('x_studio_cules_son_los_sntomas_que_presenta', row, cache, ),
            'symptoms_since': format_value('x_studio_desde_cundo_presenta_los_sntomas', row, cache, ),
            #paso2
            'previous_condition1': format_value('x_studio_previous_condition_1_1', row, cache, ),
            'previous_condition2': format_value('x_studio_2_sufre_de_alguna_condicin_o_enfermedad', row, cache, ),
            'previous_condition3': format_value('x_studio_3_sufre_de_alguna_condicin_o_enfermedad', row, cache, ),
            'last_3_hours_medication': format_value('x_studio_ha_tomado_algn_medicamento_en_las_ltimas_tres_3_horas', row, cache,True ),
            'medication_name': format_value('x_studio_nombre_de_los_medicamentos_indicados', row, cache, ),
            #paso3
            'feel_pain': format_value('x_studio_siente_usted_dolor', row, cache,True ),
            'pain_level': format_value('x_studio_pain_level_1', row, cache,True ),
            'pcp_last_visit': get_formatted_dt(pcp_last_visit),
            'child_vaccination': format_value('x_studio_se_encuentran_al_da_las_vacunas_del_nio', row, cache,True ),
            #paso4
            'beneficiary_recommendation': format_value('x_studio_recomendacion_para_el_beneficiario', row, cache,True ),
            'nutritionist': format_value('x_studio_referido_a_nutricionista', row, cache,True ),
            'emergency_room': format_value('x_studio_sala_de_emergencia_a_la_cual_se_refiri', row, cache,True ),
            'municipal_emergency_room': get_emergency_room(db_info, cache,format_value('x_studio_municipio_de_la_sala_de_emergencia', row, cache, )),
            'nurse_id': get_nurse(cache,format_value('x_studio_nombre_de_la_enfermera_que_atendi_el_caso', row, cache, )),
            'triage_info': format_value('x_studio_triage_information_1', row, cache, ),
            'call_ended': get_formatted_dt(call_ended,True),

        # insalud Censo
            'production_date': format_value('x_studio_production_date', row, cache, ),
            'pcp_npi': format_value('x_studio_pcp_npi', row, cache, ),

        # desafilicion for alianza, insalud, RSE
            'razon_desafiliacion': format_value('x_studio_razn_desafiliacin', row, cache, True),
            'estatus_boleta': format_value('x_studio_estatus_boleta', row, cache,True ),

        # Telemedicina for alianza
            'use_phone_or_pc': format_value('x_studio_tiene_y_sabe_usar_celular_tableta_o_pc_inteligente', row, cache,True ),
            'has_email': format_value('x_studio_paciente_tiene_correo_electrnico', row, cache, True),
            'has_signal': format_value('x_studio_paciente_tiene_buena_seal_de_celular_en_su_hogar', row, cache,True ),
            'has_internet': format_value('x_studio_paciente_tiene_internet_en_su_hogar', row, cache,True ),
            'has_high_speed_internet': format_value('x_studio_paciente_tiene_internet_de_alta_calidad', row, cache,True ),
            'receive_services': format_value('x_studio_paciente_interesa_recibir_servicios_de_telemedicina', row, cache,True ),
            'inform_patient_portal': format_value('x_studio_paciente_informado_sobre_portal_de_paciente_del_centro', row, cache,True ),
            'used_patient_portal': format_value('x_studio_paciente_ha_usado_el_portal_de_pacientes', row, cache,True ),
        
            # encuestas for insalud
            'encuesta': format_value('x_studio_encuesta', row, cache,True ),
            'encuesta_question1': format_value('x_studio_encuesta_pregunta_01', row, cache,True ),
            'encuesta_question2': format_value('x_studio_encuesta_pregunta_02', row, cache,True ),
            'encuesta_question3': format_value('x_studio_encuesta_pregunta_03', row, cache,True ),
            'encuesta_question4': format_value('x_studio_encuesta_pregunta_04', row, cache,True ),
        }

        create_list.append(contact)
    end = time.time()
    print ("End setting up: %d (%d)" % (end, (end - start)))
    return process_batch(create_list, db_info)


def process_batch(create_list, db_info):
    start2 = time.time()
    print ("Start creating method: ", start)
    new_ids = db_info.models.execute_kw(db_info.db, db_info.uid, db_info.password,
            'helpdesk.ticket', 'create', [create_list], {'context': {'no_update': True}})
    end2 = time.time()
    print ("End creating: %d (%d)" % (end2, (end2 - start2)))
    print("Created tickets: ",new_ids)

    logging.info('Sucessfully created batch of: ' + str(len(new_ids)))
    # end = time.time()
    # print ("End time: %d (%d)" % (end, (end - start)))
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
    for i in range(16200, len(data), batch):
        yield data[i:i + batch]



def run():

    db_info = dbInfo(url, db, username, password)
    # Log in verification and user id/models var
    if_access = log_verification(db_info)

    path = 'cra_helpdesk_ticket24.csv'
    with open(path, newline="") as csvfile:
        reader = csv.reader(csvfile, delimiter=",")
        data = process_file(reader)
        cache = create_cache(db_info)
        
        count =0
        tot = len(list(get_data_chunk(data, BATCH)))
        for d in list(get_data_chunk(data, BATCH)):
            # rec_ids = [rec.get('id') for rec in d]
            # new_ids = get_new_records(rec_ids,db_info)
            count += 1
            print ("count: %d/%d" % (count, tot))
            # start = time.time()
            # print ("Start time: ", start)
        
            db_ids = create_contact(d, cache, db_info)
            # end = time.time()
            # print ("End time: %d (%d)" % (end, (end - start)))


if __name__ == "__main__":
    run()
