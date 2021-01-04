import os, sys
from domino.core import log, Version
from domino.account import Account, find_account_id
from domino.databases.postgres import Postgres
from settings import MODULE_ID

import domino.tables.postgres.dictionary
import domino.tables.postgres.dept_param
import domino.tables.postgres.dept
#import domino.tables.postgres.server 
import domino.tables.postgres.user
import domino.tables.postgres.grant
import domino.tables.postgres.request_log 

#import tables.postgres.good_param
import domino.tables.postgres.good
#import domino.tables.postgres.organization

import procs.load_goods

class OnActivateLog:
    def __init__(self, account_id):
        self.account_id = account_id
    def __call__(self, msg): 
        print(msg)

if __name__ == "__main__":
    try:
        _account_id = sys.argv[1]
    except:
        print('НЕ ЗАДАНА УЧЕТНАЯ ЗАПИСЬ')
        sys.exit(1)
    
    account_id = find_account_id(_account_id)
    if not account_id:
        print(f'НЕ НАЙДЕНА УЧЕТНАЯ ЗАПИСЬ {_account_id}')
        sys.exit(1)

    msg_log = OnActivateLog(account_id)
    
    #Postgres.create_database(account_id, msg_log)
    Postgres.on_activate(account_id, msg_log)

    domino.tables.postgres.user.on_activate(account_id, msg_log)
    domino.tables.postgres.dictionary.on_activate(account_id, msg_log)
    domino.tables.postgres.dept_param.on_activate(account_id, msg_log)
    #domino.tables.postgres.dept.on_activate(account_id, msg_log)
    #domino.tables.postgres.server.on_activate(account_id, msg_log) 
    #domino.tables.postgres.request_log.on_activate(account_id, msg_log) 
    #tables.postgres.good.on_activate(account_id, msg_log)
    
    procs.load_goods.on_activate(account_id, msg_log)
    #procs.cleaning.on_activate(account_id, msg_log)
   
