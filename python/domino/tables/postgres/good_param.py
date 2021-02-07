import json, datetime, os, sys
from sqlalchemy import Column, Index, BigInteger, Integer, String, JSON, Boolean, DateTime, text as T, or_, and_

from domino.core import log
from domino.databases.postgres import Postgres
from domino.tables.postgres.dictionary import Dictionary
 
class GoodParam(Postgres.Base):

    __tablename__ = 'good_param'

    column_id       = Column('column_id',String, primary_key=True, nullable=False)
    disabled        = Column(Boolean)
    name            = Column(String)
    info            = Column(JSON)
    
    @property
    def id(self):
        return self.column_id
    
    @property
    def is_available(self):
        return self.disabled is None or self.disabled == False

    @staticmethod
    def availables(postgres):
        return postgres.query(GoodParam).filter(or_(GoodParam.disabled == False, GoodParam.disabled == None)).all()

    def items(self, postgres):
        return postgres.query(Dictionary.code, Dictionary.name)\
            .filter(Dictionary.CLASS == 'good', Dictionary.TYPE == self.id)\
            .order_by(Dictionary.name).all()
    
    def get_item(self, postgres, code):
        return postgres.query(Dictionary.name)\
            .filter(Dictionary.CLASS == 'good', Dictionary.TYPE == self.id)\
            .filter(Dictionary.code == code)\
            .first()

    def add_item(self, postgres, code, name):
        item = self.get_item(postgres, code)
        if item:
            #item.name = name
            return item
        else:
            item = Dictionary(CLASS = 'good', TYPE = self.id, name=name, code=code)
            postgres.add(item)
        return item

    def delete_item(self, postgres, code):
        postgres.query(Dictionary)\
            .filter(Dictionary.CLASS == 'good',  Dictionary.TYPE == self.id, Dictionary.code == code)\
            .delete()

    def get_name(self, postgres, code):
        r = postgres.query(Dictionary.name)\
            .filter(Dictionary.CLASS == 'good', Dictionary.TYPE == self.id)\
            .filter(Dictionary.code == code)\
            .first()
        return r[0] if r else None

    def __repr__(self):
        return f'GoodParam({self.id}, {self.name}, {self.disabled})'

GoodParamTable = GoodParam.__table__
Postgres.Table(GoodParamTable)
