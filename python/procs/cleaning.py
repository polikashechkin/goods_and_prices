# -*- coding: utf-8 -*-
import os, sys, datetime, time, sqlite3, json, re, enum

path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if path not in sys.path:
    sys.path.append(path)

from sqlalchemy import insert, update, delete, select
from domino.jobs import Proc
from domino.core import log, DOMINO_ROOT, RawToHex, HexToRaw
#from domino.databases.oracle import Databases, Oracle
from domino.databases.postgres import Postgres
from domino.tables.postgres.good import Good, GoodTable
from domino.tables.postgres.good_param import GoodParam, GoodParamTable
from settings import MODULE_ID
from domino.pages import Page as BasePage
from domino.pages import Title, Text
from domino.enums.unit import Unit
from domino.enums.country import Country

DESCRIPTION = 'Реорганизация данных'
PROC_ID = 'procs/cleaning.py'

def on_activate(account_id, on_activate_log):
    Proc.create(account_id, MODULE_ID, PROC_ID, description=DESCRIPTION, url='procs/cleaning')

class Page(BasePage):
    def __init__(self, application, request):
        super().__init__(application, request)
        self.proc = Proc.get(self.account_id, MODULE_ID, PROC_ID)

    def __call__(self):
        self.title(DESCRIPTION)
        p = Text(self, 'about')
        p.text('''
        Процедура анализирует данные, корректирует их при необходимости,
        удаляет потерявщие актуальность данные и т.п.
        ''')
        
class TheJob(Proc.Job):
    def __init__(self, ID):
        super().__init__(ID)

    def __call__(self):
        self.log('НАЧАЛО РАБОТЫ')
        self.postgres = Postgres.Pool().session(self.account_id)
        try:
            self.dowork()
            self.postgres.commit()
        except Exception as ex:
            self.log(f'ЗАВЕРШЕНО C ОШИБКОЙ {ex}')
            log.exception(__file__)
            raise Exception(ex)
        finally:
            self.postgres.close()
        self.log('ЗАВЕРШЕНО УСПЕШНО')

    def dowork(self):
        self.check_products()
        self.create_goods_json()

    def check_products(self):
        #self.log(f'ОПИСАНИЕ ПОЛЕЙ')
        #self.columns = GoodColumns(self.oracle, self.postgres)
        #for c in self.columns:
        #    self.log(f'{c.name.upper() if c.name else ""} : {c.db_column} : {len(c.by_uid)}')
        #    if c.db_sql :
        #        self.log(c.db_sql)

        #----------------------------------------------
        self.log(f'ПРОВЕРКА И КОРРЕКТИРОВКА СПРАВОЧНИКА ТОВАРОВ')
        count = 0
        edit_count = 0
        for good in self.postgres.query(Good):
            count += 1
            edit = 0 
            if good.t is None:
                good.t = Good.Type.ТОВАР
                edit += 1
            if edit:
                edit_count += edit
                self.postgres.commit()
        self.log(f'Обработано {count}, исправлено {edit_count}')

    def create_goods_json(self):
        self.log('СОЗДАНИЕ/ОБНОВЛЕНИЕ GOODS.JSON')
        columns = []
        for good_param in GoodParam.availables(self.postgres):
            columns.append(good_param.id)
        rows = {}
        cur = self.postgres.execute(f'select code, {", ".join(columns)} from good where state=0')
        for row in cur:
            self.check_for_break()
            code = row[0]
            rows[code] = row[1:]
        goods_json = {'columns':columns, 'goods': rows}
        NEW_GOODS_JSON = json.dumps(goods_json, ensure_ascii=False)
        self.log(f'Колонки {columns}, строк {len(rows)}, размер {len(NEW_GOODS_JSON)}')
        #----------------------------------------------
        for json_file in [
            os.path.join(DOMINO_ROOT, 'accounts', self.account_id, 'data', 'goods.json'),
            os.path.join(DOMINO_ROOT, 'accounts', self.account_id, 'data', 'discount', 'schemas','goods.json')
            ]:
            self.check_for_break()
            OLD_GOODS_JSON = None
            if os.path.isfile(json_file):
                with open(json_file) as f:
                    OLD_GOODS_JSON = f.read()
            self.log(f'{json_file.upper()}, размер {len(OLD_GOODS_JSON) if OLD_GOODS_JSON else "None"}')
            if OLD_GOODS_JSON is None or OLD_GOODS_JSON != NEW_GOODS_JSON:
                os.makedirs(os.path.dirname(json_file), exist_ok=True)
                with open(json_file, 'w') as f:
                    f.write(NEW_GOODS_JSON)
                self.log(f'Создан/обновлен {json_file}')
            else:
                self.log(f'Обновление не требуется')

if __name__ == "__main__":
    try:
        ID = sys.argv[1]
        with TheJob(ID) as job:
            job()
    except:
        log.exception(__file__)
