# -*- coding: utf-8 -*-
import os, sys, datetime, time, sqlite3, json, re, enum

path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if path not in sys.path:
    sys.path.append(path)

from sqlalchemy import insert, update, delete, select
from domino.jobs import Proc
from domino.core import log, DOMINO_ROOT, RawToHex, HexToRaw
from domino.databases.oracle import Databases, Oracle
from domino.databases.postgres import Postgres
from domino.tables.postgres.good import Good, GoodTable
from domino.tables.postgres.good_param import GoodParam, GoodParamTable
from settings import MODULE_ID
from domino.pages import Page as BasePage
from domino.pages import Title
from domino.enums.unit import Unit
from domino.enums.country import Country

DESCRIPTION = 'Обновление справочников'
PROC_ID = 'procs/load_goods.py'

#class Уценка(enum):
#    Да  = ['', "Да"]
#    Нет = NO = []
#        self.add(Good.f15073289, db_type ='codif', codif={'07D2000307D20001': 'Нет', '07D2000307D20002': 'Да'}) # 'Уценка', 'db_type':"codif", 'codif': {'07D2000307D20001': 'Нет', '07D2000307D20002': 'Да'}}) #',    "name" : 'Уценка',          'type':"codif", 'codif': {'07D2000307D20001': 'Нет', '07D2000307D20002': 'Да'} },

def on_activate(account_id, on_activate_log):
    Proc.create(account_id, MODULE_ID, PROC_ID, description='Обновление справочников', url='procs/load_goods')

class Page(BasePage):
    def __init__(self, application, request):
        super().__init__(application, request)
        self.proc = Proc.get(self.account_id, MODULE_ID, PROC_ID)

    def __call__(self):
        self.title(DESCRIPTION)
        p = self.text_block()
        p.text('''
        При импорте справичниов из Домино не всегда необходимо загружать все товары. Справочник в может быть 
        избыточен за счет товаров, потерявших актуальность.
        Для фильтрации таких товаров можно задать шаблон наименования (регулярное выражение) при
        совпадении с которым товар считается выбывшим из ассортимента.
        ''')
        self.print_toolbar()

    def print_toolbar(self):
        toolbar = self.toolbar('toolbar').mt(1)
        toolbar.item().input(label='Шаблон наименования', name='pattern', value=self.proc.info.get('pattern'))\
            .onkeypress(13, '.on_change', forms=[toolbar])

    def on_change(self):
        pattern = self.get('pattern')
        self.proc.info['pattern'] = pattern
        self.proc.save()
        self.print_toolbar()
        self.message(f'Шаблон наименования "{pattern}"')

E_CODE = 0
DB_VALUE = 0
CODE = 1
NAME = 2

class GoodColumn:
    def __init__(self, column, db_column_name = None, db_column = None, db_type = None, codif=None):
        self.column = column
        self.id = self.column.name 
        self.db_column_name = db_column_name
        self.by_code = {}
        self.by_uid = {}
        self.max_code = 0
        self.pg_column = self.column.name 
        self.db_type = db_type
        self.codif = codif
        self.width = self.column.info.get('width')
        self.is_dictionary = bool(self.db_type)
        self.name = self.column.info.get('name')
        self.class_id = 'good'
        self.type_id = self.id

        self.db_column = db_column
        if not self.db_column:
            if self.db_type:
                if self.db_type == 'classif':
                    self.db_column = f'domino.DominoUIDToString(p.{self.id}) as "{self.id}"'
                elif self.db_type == 'codif':
                    self.db_column = f'rawtohex(p.{self.id}) as "{self.id}"'
                elif self.db_type in ['unit', 'country']:
                    self.db_column = f'p.{self.db_column_name} as "{self.id}"'
                elif self.db_type == 'group':
                    self.db_column = f'domino.DominoUIDToString(p.{self.db_column_name}) as "{self.id}"'
                else:
                    raise Exception(f'НЕИЗВЕСТНЫЙ ТИП {self.db_type}')
            else:
                self.db_column = f'p.{self.db_column_name} as "{self.id}"'

    def __str__(self):
        return self.id
    def __repr__(self):
        return self.id
    
    def create_dictionary(self, postgres):
        if self.is_dictionary:
            good_param = postgres.query(GoodParam).get(self.id)
            if not good_param:
                good_param = GoodParam(column_id=self.id, name=self.name, disabled=True)
                postgres.add(good_param)
                postgres.commit()

        sql = "select row_id from dictionary where class_id= 'good' and type_id='column' and e_code=:type_id"
        cur = postgres.execute(sql, {'type_id':self.id})
        r = cur.fetchone()
        if r is None:
            sql = "insert into dictionary (class_id, type_id, e_code, code, name) values('good', 'column', :e_code, :code, :name)"
            postgres.execute(sql, {'e_code': self.id, 'code': self.id, 'name':self.name})

    @property
    def db_sql(self):
        if self.db_type:
            if self.db_type == 'classif':
                if self.id == 'f61669377' : # Старый поставщик
                    return "select code || ' ' || name as name from db1_classif where id = domino.StringToDominoUID(:value)"
                else:
                    return "select name from db1_classif where id = domino.StringToDominoUID(:value)"

            elif self.db_type == 'group':
                return "select c.name || ' : ' || g.name as name from db1_classif c, db1_classif g where c.pid = g.id and c.id=domino.StringToDominoUID(:value)"

    def get_entry(self, db_value):
        return self.by_uid.get(db_value)

    def get_name(self, oracle, db_value):
        if self.db_type:
            if self.db_type == 'codif':
                return self.codif.get(db_value)
            elif self.db_type == 'unit':
                unit = Unit.get(db_value)
                return unit.name if unit else None
            elif self.db_type == 'country':
                country = Country.get(db_value)
                return country.name if country else None
            else:
                r = oracle.execute(self.db_sql, {'value':db_value}).fetchone()
                return r[0] if r is not None else None
        return None

    def create_entry(self, oracle, postgres, db_value):
        if db_value is None:
            return None
        if self.db_type == 'unit':
            unit = Unit.get(db_value)
            if unit:
                code, name = unit.code_name 
            else:
                log.debug(f'UNIT ? {RawToHex(db_value)}')
                return None
        elif self.db_type == 'country':
            country = Country.get(db_value)
            if country:
                code, name = country.code_name
            else:
                log.debug(f'COUNTRY ? {RawToHex(db_value)}')
                return None
        else:
            self.max_code += 1
            code = self.max_code
            name = self.get_name(oracle, db_value)
            if name is None:
                return None
        entry = self._create_entry(db_value, code, name)
        #entry = [db_value, code, name]
        #self.by_uid[db_value] = entry
        #self.by_code[code] = entry
        #sql = 'insert into "dictionary" (class_id, type_id, e_code, code, "name") values (:class_id, :type_id, :db_value, :code, :name) RETURNING "row_id"'
        sql = 'insert into "dictionary" (class_id, type_id, e_code, code, "name") values (:class_id, :type_id, :db_value, :code, :name)'
        postgres.execute(sql, {'class_id':self.class_id, 'type_id':self.type_id, 'db_value':db_value, 'code':code, 'name':name})
        return entry

    def _create_entry(self, db_value, code, name):
        if code.isdigit():
            icode = int(code)
            if icode > self.max_code:
                self.max_code = icode
        entry = [db_value, code, name]
        self.by_code[code] = entry
        self.by_uid[db_value] = entry
        return entry

    def load_dictionary(self, postgres):
        sql = 'select "e_code", "code", "name" from dictionary where class_id=:class_id and type_id=:type_id'
        cur = postgres.execute(sql, {'class_id':'good', 'type_id':self.id})
        for db_value, code, name in cur:
            self._create_entry(db_value, code, name)

    @property
    def size(self):
        if self.is_dictionary:
            return len(self.by_uid)

    def convert(self, oracle, postgres, db_value):
        #log.debug(f'{db_value}')
        if db_value is None:
            return None, None
        if self.is_dictionary:
            entry = self.get_entry(db_value)
            if entry is not None:
                return entry[CODE], entry[NAME]
            else:
                #name = self.get_name(oracle, db_value)
                #if name is None:
                #    return None, None
                #else:
                entry = self.create_entry(oracle, postgres, db_value)
                if entry:
                    return entry[CODE], entry[NAME]
                else:
                    return None, None
        else:
            return db_value, None
    
    def check_dictionary(self, postgres):
         if self.is_dictionary:
            sql = "update dictionary set state=:state where class_id='good' and type_id='column' and code=:code"
            postgres.execute(sql, {'code':self.id, 'state':0 if self.size else 1})

            good_param = postgres.query(GoodParam).get(self.id)
            good_param.disabled = not bool(self.size)

class GoodColumns:
    def __init__(self, oracle, postgres):
        self.oracle = oracle
        self.postgres = postgres

        self.available_columns = set()
        sql = "select column_name from user_tab_columns where table_name = 'DB1_PRODUCT' "
        for column_id, in self.oracle.execute(sql):
             self.available_columns.add(column_id.lower())

        self.columns = []
        self.add(Good.code)
        self.add(Good.name)
        self.add(Good.local_group, db_type='group')
        self.add(Good.f2818049,  db_type ='classif')
        self.add(Good.f28835913, db_type ='classif') # Ценовая ниша', 'db_type':"classif"
        self.add(Good.f15073289, db_type ='codif', codif={'07D2000307D20001': 'Нет', '07D2000307D20002': 'Да'}) # 'Уценка', 'db_type':"codif", 'codif': {'07D2000307D20001': 'Нет', '07D2000307D20002': 'Да'}}) #',    "name" : 'Уценка',          'type':"codif", 'codif': {'07D2000307D20001': 'Нет', '07D2000307D20002': 'Да'} },
        self.add(Good.f42401855, db_type = 'classif') #  = Column(String, info = {"name" : 'Основной цвет',   'db_type':"classif"}) #',    "name" : 'Основной цвет',   'type':"classif"},
        self.add(Good.f15073282, db_type ='classif') #  = Column(String, info = {"name" : 'Основной материал изготовления',   'db_type':"classif"}) #',    "name" : 'Основной материал изготовления',   'type':"classif"},
        self.add(Good.f42401857, db_type ='classif') # = Column(String, info = {"name" : 'Размер',   'db_type':"classif"}) #',    "name" : 'Размер',   'type':"classif"},
        self.add(Good.f62455820, db_type ='classif') #   = Column(String, info = {"name" : 'Гендер',   'db_type':"classif"}) #',    "name" : 'Гендер',   'type':"classif"},
        self.add(Good.f62455821, db_type ='classif') # = Column(String, info = {"name" : 'Целевая аудитория',   'db_type':"classif"})#',    "name" : 'Целевая аудитория',   'type':"classif"},
        self.add(Good.f62455822, db_type ='classif') #  = Column(String, info = {"name" : 'Назначение',   'db_type':"classif"}) #',    "name" : 'Назначение',   'type':"classif"},
        self.add(Good.f61931523, db_type ='classif') #  = Column(String, info = {"name" : 'Категория для интернет магазина',   'db_type':"classif"}) #',    "name" : 'Категория для интернет магазина',   'type':"classif"},
        self.add(Good.f61669377, db_type ='classif') # = Column(String, info = { "name" : 'Старый постащик',   'db_type':"classif"}) #',    "name" : 'Старый постащик',   'type':"classif"}
        self.add(Good.uid, db_column_name='id')
        self.add(Good.unit, db_column_name='f14745607', db_type='unit')
        self.add(Good.country, db_column_name='f14745604', db_type='country')

    def add(self, column , db_type=None, codif=None, db_column_name=None):
        db_column_name = db_column_name if db_column_name else column.name
        if db_column_name in self.available_columns:
            good_column = GoodColumn(column, db_type=db_type, codif=codif, db_column_name = db_column_name)
            self.columns.append(good_column)
            good_column.create_dictionary(self.postgres)
            good_column.load_dictionary(self.postgres)
    
    def convert(self, db_row):
        good = {}
        description = {}
        info = {}
        nn = 0
        for db_value in db_row:
            if db_value is not None:
                column = self.columns[nn]
                if column.is_dictionary:
                    value, value_name = column.convert(self.oracle, self.postgres, db_value)
                    if value:
                        info[column.id] = value
                        good[column.id] = value
                        if column.name and value_name:
                            description[column.name] = value_name
                else:
                    good[column.id] = db_value
            nn += 1
        good['description'] = description
        good['info'] = info
        return good['code'], good

    def check_dictionaries(self):
        for column in self.columns:
            if column.is_dictionary:
                column.check_dictionary(self.postgres)
        self.postgres.commit()

    def __str__(self):
        names = []
        for column in self.columns:
            names.append(column.id)
        return ', '.join(names)

    def __iter__(self):
        return self.columns.__iter__()

    def __getitem__(self, index):
        return self.columns[index]

class TheJob(Proc.Job):
    def __init__(self, ID):
        #log.info(f'Запуск задачи {ID} : load')
        super().__init__(ID)
        #self.db_connection = None
        #self.db_cursor = None
        #self.connection = None
        #self.cursor = None
        #self.database = None

    def __call__(self):
        self.log('НАЧАЛО РАБОТЫ')

        #self.database = Databases().get_database(self.account_id)
        #self.db_connection = self.database.connect()
        #self.db_cursor = self.db_connection.cursor()
        #self.pg_connection = Postgres.connect(self.account_id)
        #self.pg_cursor = self.pg_connection.cursor()

        self.oracle = Oracle.Pool().session(self.account_id)
        self.postgres = Postgres.Pool().session(self.account_id)
        try:
            self.dowork()
            self.postgres.commit()
        except Exception as ex:
            self.log(f'ЗАВЕРШЕНО C ОШИБКОЙ {ex}')
            log.exception(__file__)
            raise Exception(ex)
        finally:
            self.oracle.close()
            self.postgres.close()

        #if self.db_connection is not None:
        #    self.db_connection.close()
        #if self.pg_connection is not None:
        #    self.pg_connection.close()

        self.log('ЗАВЕРШЕНО УСПЕШНО')

    def dowork(self):
        pattern = self.info.get('pattern')
        self.pattern = None
        if pattern:
            self.log(f'Шаблон наименования "{pattern}"')
            self.pattern = re.compile(pattern)

        self.load_products()
        self.check_for_break()
        self.create_goods_json()
        self.check_for_break()

    def old_name(self, name):
        return self.pattern is not None and self.pattern.match(name) is not None

    def load_products(self):
        self.log(f'ОПИСАНИЕ ПОЛЕЙ')
        self.columns = GoodColumns(self.oracle, self.postgres)
        for c in self.columns:
            self.log(f'{c.name.upper() if c.name else ""} : {c.db_column} : {len(c.by_uid)}')
            if c.db_sql :
                self.log(c.db_sql)

        #----------------------------------------------
        self.log(f'ЗАГРУЗКА И КОНВЕРТИРОВАНИЕ')
        db_columns = []
        for column in self.columns:
            db_columns.append(column.db_column)
        #db_select = f'select {" ,".join(db_columns)} from db1_product p, db1_classif g where p.type in (14745601,62455812) and p.class=14745601 and p.local_group  = g.id and g.type=14745602'
        db_select = f'select {" ,".join(db_columns)} from db1_product p, db1_classif g where p.class=14745601 and p.local_group  = g.id and g.type=14745602 and p.name is not null'
        #db_select = f'select {" ,".join(db_columns)} from db1_product p, db1_classif g where p.type = 14745601 and p.class=14745601 and p.local_group  = g.id and g.type=14745602 and p.f2818049 is not null'
        #db_select = f'select {" ,".join(db_columns)} from db1_product p, db1_classif g where p.local_group  = g.id and g.type=14745602'
        self.log(f'{db_select}')
        #cur = self.db_connection.cursor()
        cur = self.oracle.execute(db_select)
        #columns = [col[0].lower() for col in cur.description]
        #cur.rowfactory = lambda *args: dict(zip(self.columns, args))
        goods = {}
        #db_rows = cur.fetchmany(10)
        #db_rows = cur.fetchall()
        nn = 0
        for db_row in cur:
            self.check_for_break()
            nn += 1
            if nn % 10000 == 0:
                self.log(nn)
            #if nn < 5:
            #    self.log(db_row)
            code, good = self.columns.convert(db_row)
            old_good = goods.get(code)
            if old_good:
                self.log(f'ДОБЛИРОВАНИЕ КОДА ТОВАРА "{code}" {RawToHex(old_good["uid"])} {RawToHex(good["uid"])}')
                #self.log(old_good)
                #self.log(good)
            else:
                goods[code] = good
                #if nn < 5:
                #    self.log(good)
        self.log(f'Обработано {nn} строк, конвертировано {len(goods)}')
        #----------------------------------------------
        self.log('ОБНОВЛЕНИЕ')
        old = 0
        count = 0
        updated = 0
        created = 0
        for code, values in goods.items():
            self.check_for_break()
            count += 1
            name = values['name']
            r = self.postgres.execute('select name, description, info from good where code=:code', {'code':code}).fetchone()
            if r:
                g_name, g_description, g_info = r
                if self.old_name(name):
                    old += 1
                    good.state = -1
                    # Пометить товар как потерявщий актуальность 
                    #self.pg_cursor.execute('update good set state=-1 where e_code=%s', [e_code])
                else:
                    description = values['description']
                    info = values['info']
                    if count < 5:
                        self.log(f'{g_info} {info} {description}')
                    if g_name != name or g_description != description or g_info != info:
                        UPDATE = GoodTable.update().where(GoodTable.c.code==code).values(values).values({'modify_time':datetime.datetime.now()})
                        self.postgres.execute(UPDATE)
                        updated += 1
            else:
                if self.old_name(name):
                    old += 1
                else:
                    INSERT = GoodTable.insert().values(values).values({'modify_time':datetime.datetime.now()})
                    self.postgres.execute(INSERT)
                    created += 1
            if count % 10000 == 0:
                self.log(count)
                self.postgres.commit()
        
        self.postgres.commit()
        self.log(f'Обработано "{count}" товаров, обновлено {updated}, создано {created}, устаревших {old}')
        #----------------------------------------------
        self.log('СПРАВОЧНИКИ')
        for column in self.columns:
            if column.is_dictionary:
                if column.size:
                    self.log(f'{column.name} : {column.size}')

        self.columns.check_dictionaries()
    
    def create_goods_json(self):
        self.log('СОЗДАНИЕ/ОБНОВЛЕНИЕ GOODS.JSON')
        columns = []
        for good_param in self.postgres.query(GoodParam).filter(GoodParam.disabled==False):
            columns.append(good_param.column_id)
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

