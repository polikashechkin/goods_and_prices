#< import 
import sys
import os
from flask import request, Flask
from domino.core import log
from domino.application import Application
from navbar import navbar
from domino.databases.postgres import Postgres
from domino.databases.oracle import Oracle
                 
POSTGRES = Postgres.Pool()
ORACLE = Oracle.Pool()
               
app = Flask(__name__)
application = Application(os.path.abspath(__file__), framework='MDL')
application['navbar'] = navbar
   
#-------------------------------------------    
import domino.pages.granted_users
@app.route('/domino/pages/granted_users', methods=['POST', 'GET'])
@app.route('/domino/pages/granted_users.<fn>', methods=['POST', 'GET'])
def _domino_pages_granted_users(fn = None):
    return application.response(request, domino.pages.granted_users.Page, fn, [POSTGRES])

import domino.pages.module_history
@app.route('/domino/pages/module_history', methods=['POST', 'GET'])
@app.route('/domino/pages/module_history.<fn>', methods=['POST', 'GET'])
def _domino_pages_module_history(fn = None):
    return application.response(request, domino.pages.module_history.Page, fn)

import domino.pages.procs
@app.route('/domino/pages/procs', methods=['POST', 'GET'])
@app.route('/domino/pages/procs.<fn>', methods=['POST', 'GET'])
def _domino_pages_procs(fn = None):
    return application.response(request, domino.pages.procs.Page, fn)
   
import domino.pages.proc_shedule
@app.route('/domino/pages/proc_shedule', methods=['POST', 'GET'])
@app.route('/domino/pages/proc_shedule.<fn>', methods=['POST', 'GET'])
def _domino_pages_proc_shedule(fn = None):
    return application.response(request, domino.pages.proc_shedule.Page, fn)
  
import domino.pages.jobs
@app.route('/domino/pages/jobs', methods=['POST', 'GET'])
@app.route('/domino/pages/jobs.<fn>', methods=['POST', 'GET'])
def _domino_pages_jobs(fn = None):
    return application.response(request, domino.pages.jobs.Page, fn)

import domino.pages.job
@app.route('/domino/pages/job', methods=['POST', 'GET'])
@app.route('/domino/pages/job.<fn>', methods=['POST', 'GET'])
def _domino_pages_job(fn = None):
    return application.response(request, domino.pages.job.Page, fn)

import domino.responses.job
@app.route('/domino/job', methods=['POST', 'GET'])
@app.route('/domino/job.<fn>', methods=['POST', 'GET'])
def _domino_responses_job(fn=None):
    return application.response(request, domino.responses.job.Response, fn)
#-------------------------------------------    
import pages.start_page
@app.route('/pages/start_page', methods=['POST', 'GET'])
@app.route('/pages/start_page.<fn>', methods=['POST', 'GET'])
def _pages_start_page(fn=None):
    return application.response(request, pages.start_page.Page, fn)
             
import pages.groups 
@app.route('/pages/groups', methods=['POST', 'GET'])
@app.route('/pages/groups.<fn>', methods=['POST', 'GET'])
def _pages_groups(fn=None):
    return application.response(request, pages.groups.Page, fn, [POSTGRES])

import pages.group 
@app.route('/pages/group', methods=['POST', 'GET'])
@app.route('/pages/group.<fn>', methods=['POST', 'GET'])
def _pages_group(fn=None):
    return application.response(request, pages.group.Page, fn, [POSTGRES])

import pages.group_create 
@app.route('/pages/group_create', methods=['POST', 'GET'])
@app.route('/pages/group_create.<fn>', methods=['POST', 'GET'])
def _pages_group_create(fn=None):
    return application.response(request, pages.group_create.Page, fn, [POSTGRES])

import pages.goods 
@app.route('/pages/goods', methods=['POST', 'GET'])
@app.route('/pages/goods.<fn>', methods=['POST', 'GET'])
def _pages_goods(fn=None):
    return application.response(request, pages.goods.Page, fn, [POSTGRES])
                              
import pages.good
@app.route('/pages/good', methods=['POST', 'GET'])
@app.route('/pages/good.<fn>', methods=['POST', 'GET'])
def _pages_good(fn=None):
    return application.response(request, pages.good.Page, fn, [POSTGRES])
 
import pages.good_create
@app.route('/pages/good_create', methods=['POST', 'GET'])
@app.route('/pages/good_create.<fn>', methods=['POST', 'GET'])
def _pages_good_create(fn=None):
    return application.response(request, pages.good_create.Page, fn, [POSTGRES])
        
import pages.units 
@app.route('/pages/units', methods=['POST', 'GET'])
@app.route('/pages/units.<fn>', methods=['POST', 'GET'])
def _pages_units(fn=None):
    return application.response(request, pages.units.Page, fn, [POSTGRES])

import pages.countries
@app.route('/pages/countries', methods=['POST', 'GET'])
@app.route('/pages/countries.<fn>', methods=['POST', 'GET'])
def _pages_countries(fn=None):
    return application.response(request, pages.countries.Page, fn, [POSTGRES])
      
import pages.good_params
@app.route('/pages/good_params', methods=['POST', 'GET'])
@app.route('/pages/good_params.<fn>', methods=['POST', 'GET'])
def _pages_good_params(fn=None):
    return application.response(request, pages.good_params.Page, fn, [POSTGRES])
                
import pages.good_param
@app.route('/pages/good_param', methods=['POST', 'GET'])
@app.route('/pages/good_param.<fn>', methods=['POST', 'GET'])
def _pages_good_param(fn=None):
    return application.response(request, pages.good_param.Page, fn, [POSTGRES])
   
import procs.load_goods
@app.route('/procs/load_goods', methods=['POST', 'GET'])
@app.route('/procs/load_goods.<fn>', methods=['POST', 'GET'])
def _procs_load_goods(fn=None):
    return application.response(request, procs.load_goods.Page, fn)

import procs.cleaning
@app.route('/procs/cleaning', methods=['POST', 'GET'])
@app.route('/procs/cleaning.<fn>', methods=['POST', 'GET'])
def _procs_cleaning(fn=None):
    return application.response(request, procs.cleaning.Page, fn)

