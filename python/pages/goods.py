from domino.core import log
from domino.pages import Page as BasePage
from domino.pages import Title, Table, Input, DeleteIconButton, TextWithComments, Toolbar, Select, Button, Text, IconButton
from domino.pages import Chip
from domino.tables.postgres.good import Good
from domino.tables.postgres.good import GoodParam
from domino.tables.postgres.dictionary import Dictionary
from domino.tables.postgres.user import User, UserTable

class GoodQueryStorage:
    def __init__(self, page):
        self.postgres = page.postgres
        self.user_id = page.user_id

    def load(self):
        self.user = self.postgres.query(User).get(self.user_id)
        query = self.user.info.get('good_query', {}) if self.user.info else {}
        #log.debug(f'LOAD : {query}')
        return query
    
    def save(self, query):
        #log.debug(f'SAVE {query}')
        self.user.info['good_query'] = query
        self.postgres.query(User).filter(User.id == self.user.id).update({UserTable.c.info : self.user.info})
        self.postgres.commit()

class GoodQuery:
    def __init__(self, page, onchange, onchange_query):
        self.page = page
        self.storage = GoodQueryStorage(page)
        #self._onchange = onchange
        #self._onchange_query = onchange_query
        self._query = None
        self.params = {}
        for param in page.postgres.query(GoodParam).filter(GoodParam.disabled==False).all():
            self.params[param.column_id] = param
    
    @property
    def query(self):
        if self._query is None:
            self._query = self.storage.load()
            #log.debug(f'LOAD_QUERY : {self._query}')
        return self._query

    def save(self):
        self.storage.save(self.query)

    def onchange_query(self):
        self.add_or_remove_column(self.page.get('column_id'))

    def add_or_remove_column(self, column_id):
        if column_id in self.query:
            del self.query[column_id]
        else:
            self.query[column_id] = ''
        self.save()

    def save_query_values(self):
        for param in self.used_params():
            value = self.page.get(param.column_id)
            if value is not None:
                self.query[param.column_id] = value
        self.save()

    def get(self, column_id):
         return self.query.get(column_id)

    def filter(self, query):
        return query.filter(Good.filter(self.query))
 
    def used_params(self):  
        params = [] 
        for column_id in self.query:
            params.append(self.params[column_id])
        return params

    def unused_params(self):
        params = []
        for param in self.params.values():
            if param.column_id not in self.query:
                params.append(param)
        return params

    def get_options(self, param):
        #options = [['','']]
        #options.append(['','<ВСЕ>'])
        #options.append() [''] + 
        #for code, name in self.page.postgres.query(Dictionary.code, Dictionary.name)\
        #    .filter(Dictionary.CLASS == 'good', Dictionary.TYPE == param.column_id)\
        #    .order_by(Dictionary.name):
        #    options.append([code, name])
        
        return [['','']] + param.items(self.page.postgres)
        #for item in param.items(self.page.postgres):
        #    options.append(item)
        #return options

class Page(BasePage):
    def __init__(self, application, request):
        super().__init__(application, request)
        self.postgres = None
        self._good_query = None
    
    @property
    def good_query(self):
        if self._good_query is None:
            self._good_query = GoodQuery(self, onchange='.onchage_query_values', onchange_query = '.onchange_query')
        return self._good_query

    def onchange_query(self):
        self.good_query.onchange_query()
        self.print_toolbar()
        self.print_table()
    
    def onchange_query_values(self):
        self.good_query.save_query_values()
        self.print_table()

    def print_toolbar(self):
        t1 = Toolbar(self, 'good_query', style='flex-wrap: wrap; align-items: flex-end; ')
        t2 = Toolbar(self, 'toolbar').mb(0.5).style('align-items:center; -justify-content: center;')

        used_params = self.good_query.used_params() 
        if len(used_params):
            for param in used_params:
                item = t1.item(mt=0.5, mr=0.5, mb=0.5)
                g = item.input_group()
                #g.small()
                g.text(param.name)
                #IconButton(g, None, 'check')
                select = g.select(name=param.column_id, value=self.good_query.get(param.column_id), 
                    options = [['','']] + param.items(self.postgres)
                    )
                #select = Select(item, label=param.name, name=param.column_id, value=self.good_query.get(param.column_id))
                select.onchange('.onchange_query_values', forms=[t1, t2])
                #for name, value in self.good_query.get_options(param):
                #    select.option(name,value)
        btn = Button(t1.item(mt=0.5, mr=0.5, mb=0.5),'Критерии').style('-background-color:lightgray')
        for param in self.good_query.used_params():
            btn.item(f'- {param.name}').onclick('.onchange_query', {'column_id':param.column_id}, forms=[])
        for param in self.good_query.unused_params():
            btn.item(f'+ {param.name}').onclick('.onchange_query', {'column_id':param.column_id})

        Input(t2.item(ml=0.1, mb=0.1, mt=0.1, style='width:20rem'), name='finder', placeholder='Поиск..')\
            .onkeypress(13, '.onchange_query_values', forms=[t1,t2])
        btn = Button(t2.item(ml='auto'), 'Создать').style('-color:white')

        #chip = Chip(t2.item())
        #IconButton(chip, None, 'close')
        #IconButton(chip, None, 'close')
        #IconButton(chip, None, 'close')

    def print_table(self):
        table = Table(self, 'table')
        table.column().text('#')
        table.column().text('Код')
        table.column().text('Наименование')
        query = self.postgres.query(Good)
        query = self.good_query.filter(query)
        finder = self.get('finder')
        if finder:
            query = query.filter(Good.name.ilike(f'%{finder}%'))

        for good in query.order_by(Good.name).limit(100):
            row = table.row(good.id)
            self.print_row(row, good)
    
    def print_row(self, row, good):
        row.cell().text(good.id)
        row.cell(wrap=False).text(good.code)
        TextWithComments(row.cell(), good.name, [f'{good.description}'])
        #row.cell().text(good.info)
 
    def __call__(self):
        Title(self, 'Товары')
        self.print_toolbar()
        self.print_table()
