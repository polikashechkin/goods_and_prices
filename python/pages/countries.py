from domino.core import log
from domino.pages import Page as BasePage
from domino.pages import CheckIconButton, Title, FlatTable, Table, Rows, Input, DeleteIconButton, TextWithComments, Toolbar, Select, Button, Text, IconButton
from domino.pages import EditIconButton
from domino.pages import FlatButton
#from domino.pages import Chip
from domino.tables.postgres.dictionary import Dictionary
from domino.tables.postgres.good_param import GoodParam
#from domino.tables.postgres.user import User, UserTable
from domino.enums.country import Country

class Page(BasePage):
    ID = 'pages/countries'

    def __init__(self, application, request):
        super().__init__(application, request)
        self.postgres = None
        self._param = None
        self._items = None

    @property
    def param(self):
        if self._param is None:
            self._param = self.postgres.query(GoodParam).get('country_id')
        return self._param
    @property
    def items(self):
        if self._items is None:
            self._items = {}
            for code, name in self.param.items(self.postgres):
                self._items[code]= Country.get(code)
        return self._items

    @property
    def show_all(self):
        return bool(self.USER_PAGE_STORE.get('show_all'))
    
    @show_all.setter
    def show_all(self, value):
        self.USER_PAGE_STORE.update({'show_all':bool(value)})

    def toggle_show(self):
        self.show_all = not self.show_all
        self.print_toolbar()
        self.print_table()

    def delete(self):
        id = self.get('id')
        self.postgres.query(Dictionary).filter(Dictionary.id == id).delete()
        #self.postgres.commit()
        Rows(self, 'table').row(id)
        
    def print_toolbar(self):
        finder_panel = Toolbar(self, 'finder_panel')
        #Input(finder_panel.item(), name = 'finder', placeholder='Поиск...')
        #-------------------------------------------
        btn_availables = Button(finder_panel.item(ml='auto'), 'Доступные')
        btn_all = Button(finder_panel.item(), 'Все')
        if self.show_all:
            btn_all.style('background-color:green;color:white')
            btn_availables.style('color:gray')
            btn_availables.onclick(self.toggle_show)
        else:
            btn_availables.style('background-color:green;color:white')
            btn_all.onclick(self.toggle_show)
            btn_all.style('color:gray')
        #btn = Button(finder_panel.item(ml='auto'), 'Добавить').style('background-color:green;color:white')
        #btn.onclick('pages/group_create')

    def print_table(self):
        table = FlatTable(self, 'table').mt(1)
        if self.show_all:
            for country in Country.items():
                row = table.row(country.name)
                self.print_row(row, country)
        else:
            for id, country in self.items.items():
                row = table.row(id)
                row.cell().text(f'{country.short_name.upper()} ({country.id.upper()})')
    
    def toggle_item(self):
        item_id = self.get('id')
        row = Rows(self, 'table').row(item_id)
        item = self.param.get_item(self.postgres, item_id)
        if item:
            self.param.delete_item(self.postgres, item_id)
            self.postgres.commit()
        else:
            country = Country.get(item_id)
            self.param.add_item(self.postgres, item_id, country.short_name)
            self.postgres.commit()
            self.print_row(row, country)

    def print_row(self, row, item):
        cell = row.cell(width=1)
        if item.name in self.items:
            CheckIconButton(cell, True).onclick(self.toggle_item, {'id':item.name})
            row.style('color:black; font-weight:bold;')
        else:
            row.style('color:gray')
            CheckIconButton(cell, False).onclick(self.toggle_item, {'id':item.name})
        row.cell().text(f'{item.short_name.upper()} ({item.id.upper()})')
 
    def __call__(self):
        Title(self, F'Страны')
        self.print_toolbar()
        self.print_table()
