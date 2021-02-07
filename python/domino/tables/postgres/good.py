import json, datetime, os, sys
from sqlalchemy import Column, Index, BigInteger, Binary, Integer, String, JSON, DateTime, func as F, text as T, or_, and_
from sqlalchemy.dialects.postgresql import VARCHAR
from sqlalchemy.orm import synonym
from sqlalchemy.types import TypeDecorator
from sqlalchemy.orm import synonym
from domino.core import log
from domino.databases.postgres import Postgres
from domino.tables.postgres.good_param import GoodParam
from domino.enums.unit import Unit 
from domino.enums.country import Country
from domino.enums.good_type import GoodType
 
class GOOD_TYPE(TypeDecorator):
    impl = VARCHAR
    def process_bind_param(self, value, dialect):
        return value.id if value is not None else None
    def process_result_value(self, value, dialect):
        return GoodType.get(value) if value is not None else None

#class UNIT(TypeDecorator):
#    impl = VARCHAR
#    def process_bind_param(self, value, dialect):
#        return value.name if value is not None else None
#    def process_result_value(self, value, dialect):
#        return Unit.get(value) if value is not None else None

#class COUNTRY(TypeDecorator):
#    impl = VARCHAR
#    def process_bind_param(self, value, dialect):
#        return value.name if value is not None else None
#    def process_result_value(self, value, dialect):
#        return Country.get(value) if value is not None else None

class Good(Postgres.Base):

    #Unit = Unit
    #Country = Country
    Type = GoodType

    __tablename__ = 'good'

    id              = Column('row_id', BigInteger, primary_key=True, autoincrement=True)
    state           = Column(Integer, default=0)
    e_code          = Column(String)
    uid             = Column(Binary)
    t               = Column('type', GOOD_TYPE)
    code            = Column(String)
    name            = Column(String)
    description     = Column(Postgres.JSON)
    modify_time     = Column(DateTime, default=F.current_timestamp(), onupdate=F.current_timestamp())
    info            = Column(JSON)
    
    local_group     = Column(String, info = {'name':'Категория'}) #',    'name' : 'Категория',       'type':"group"},
    f2818049        = Column('f2818049', String, info = {'name':'Торговая марка'}) #",    "name" : 'Торговая марка',  'type':"classif"},
    f28835913       = Column('f28835913', String, info = {"name":'Ценовая ниша'}) #',    "name" : 'Ценовая ниша',    'type':"classif"},
    f15073289       = Column('f15073289', String, info = {"name" : 'Уценка'}) #',    "name" : 'Уценка',          'type':"codif", 'codif': {'07D2000307D20001': 'Нет', '07D2000307D20002': 'Да'} },
    f42401855       = Column('f42401855', String, info = {"name" : 'Основной цвет'}) #',    "name" : 'Основной цвет',   'type':"classif"},
    f15073282       = Column('f15073282', String, info = {"name" : 'Основной материал изготовления'}) #',    "name" : 'Основной материал изготовления',   'type':"classif"},
    f42401857       = Column('f42401857', String, info = {"name" : 'Размер'}) #',    "name" : 'Размер',   'type':"classif"},
    f62455820       = Column('f62455820', String, info = {"name" : 'Гендер'}) #',    "name" : 'Гендер',   'type':"classif"},
    f62455821       = Column('f62455821', String, info = {"name" : 'Целевая аудитория'})#',    "name" : 'Целевая аудитория',   'type':"classif"},
    f62455822       = Column('f62455822', String, info = {"name" : 'Назначение'}) #',    "name" : 'Назначение',   'type':"classif"},
    f61931523       = Column('f61931523', String, info = {"name" : 'Категория для интернет магазина'}) #',    "name" : 'Категория для интернет магазина',   'type':"classif"},
    f61669377       = Column('f61669377', String, info = { "name" : 'Старый постащик'}) #',    "name" : 'Старый постащик',   'type':"classif"}
    unit_id         = Column('unit_id'  , String, default = 'NMB' ,  info={'name':'ЕИ'})
    country_id      = Column('country_id', String, default='RU', info={'name':'Страна'})

    Index('', code)
    Index('', uid)

    @property
    def mtime(self):
        return self.modify_time
    @property
    def unit(self):
        return Unit.get(self.unit_id)

    @property
    def country(self):
        return Country.get(self.country_id)

    @staticmethod
    def column(column_id):
        #if column_id == 'unit_id':
        #    return Good.unit
        #elif column_id == 'country_id':
        #    return Good.country
        #else:
        return getattr(Good, column_id, None)

    def get_value(self, column_id):
        #if column_id == 'unit_id':
        #    return self.unit
        #elif column_id == 'country_id':
        #    return self.country
        #else:
        return self.__dict__.get(column_id)

    def update_description(self, description):
        if self.description is None:
            self.description = {}
            for param_name, value_name in description.items():
                self.description[param_name] = value_name

    @staticmethod
    def filter(query_columns):
        #if len(query_columns) == 0:
        #    return True
        q = []
        for column_id, values in query_columns.items():
            if not values: continue
            if column_id in ['code', 'name'] : continue
            column = Good.column(column_id)
            #if column_id == 'unit_id':
            #    column = Good.unit
                #values = Unit.get(values)
            #elif column_id == 'country_id':
            #    column = Good.country
                #values = Country.get(values)
            #else:
            #    column = getattr(Good, column_id, None)
            if column is None: continue

            if isinstance(values, (list, tuple)):
                if len(values) == 1:
                    q.append(column == values[0])
                elif len(values) > 1:
                    q.append(column.in_(values))
            else:
                q.append(column == values)
        #return and_(*q) if len(q) else True
        return and_(*q)

    @staticmethod
    def nextval(postgres):
        n = postgres.execute("SELECT nextval('good_numerator')").scalar()
        return f'{n:05}'

def create_init_values(postgres, msg_log):
    #msg_log('create_init_values')
    # UNIT ----------------------------------
    column_id = Good.unit_id.name
    unit_param = postgres.query(GoodParam).get(column_id)
    if unit_param:
        if unit_param.disabled:
            unit_param.disabled = False
    else:
        unit_param = GoodParam(column_id = column_id, name=Good.unit_id.info['name'])
        postgres.add(unit_param)

    msg_log(f'unit_param.add_item(postgres, {Unit.NMB.name}, {Unit.NMB.short_name})')    
    unit_param.add_item(postgres, Unit.NMB.name, Unit.NMB.short_name)
    # COUNTRY ----------------------------------
    column_id = Good.country_id.name
    column_name = Good.country_id.info['name']
    country_param = postgres.query(GoodParam).get(column_id)
    if country_param:
        if country_param.disabled:
            country_param.disabled = False 
    else:
        country_param = GoodParam(column_id = column_id, name=column_name)
        postgres.add(country_param)
    
    country_param.add_item(postgres, 'RU', 'Россия')
    # LOCAL_GROUP ----------------------------------
    column_id = Good.local_group.name
    column_name = Good.local_group.info['name']
    group_param = postgres.query(GoodParam).get(column_id)
    if group_param:
        if group_param.disabled:
            group_param.disabled = False 
    else:
        group_param = GoodParam(column_id = column_id, name=column_name)
        postgres.add(group_param)
    
    if len(group_param.items(postgres)) == 0:
        msg_log('ПЕРВАЯ КАТЕГОРИЯ')
        group_param.add_item(postgres, '.', 'ПЕРВАЯ КАТЕГОРИЯ')
        postgres.flush()

def create_numerator(postgres, msg_log):
    sql = 'CREATE SEQUENCE IF NOT EXISTS "good_numerator"'
    #msg_log(sql)
    postgres.execute(sql)

GoodTable = Good.__table__
Postgres.Table(GoodTable).additionls(create_init_values, create_numerator)
