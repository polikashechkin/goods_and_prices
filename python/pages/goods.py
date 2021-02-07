from domino.core import log
from domino.pages import Page as BasePage
from domino.pages import Title, Table, Rows, Input, DeleteIconButton, TextWithComments, Toolbar, Select, Button, Text, IconButton
from domino.pages import EditIconButton
from domino.pages import FlatButton
#from domino.pages import Chip
from domino.tables.postgres.good import Good
#from domino.tables.postgres.good import GoodParam
#from domino.tables.postgres.dictionary import Dictionary
#from domino.tables.postgres.user import User, UserTable
from domino.components.good_view import GoodView

class Page(BasePage):
    ID = 'pages/goods'

    def __init__(self, application, request):
        super().__init__(application, request)
        self.postgres = None
        self.view = GoodView(self, self.onchange, Storage=GoodView.DefaultQueryStorage)
    
    def onchange(self):
        self.view.onchange()
        self.print_toolbar()
        self.print_table()

    def delete(self):
        good_id = self.get('good_id')
        self.postgres.query(Good).filter(Good.id == good_id).delete()
        self.postgres.commit()
        Rows(self, 'table').row(good_id)
        
    def print_toolbar(self):
        query_panel = Toolbar(self, 'query_panel')
        finder_panel = Toolbar(self, 'finder_panel').mb(0.2)
        #----------------------------------------
        self.view.print_panel(query_panel, finder_panel)
        #----------------------------------------
        btn = Button(finder_panel.item(ml='auto'), 'Создать').style('background-color:green;color:white')
        for good_type in Good.Type:
            btn.item(good_type.description).onclick('pages/good_create', {'good_type_id':good_type.id})

    def print_table(self):
        table = Table(self, 'table')
        if self.view.show_mode == self.view.SHORT:
            table.column().text('Тип')
            table.column().text('Код')
            table.column().text('Наименование')
            table.column().text('ЕИ')
            table.column().text('Посленее изменение')
        else:
            table.column().text('Код')
            table.column().text('Описание')
            table.column()

        query = self.postgres.query(Good)
        query = self.view.filter(query)

        for good in query.order_by(Good.name).limit(100):
            row = table.row(good.id)
            self.print_row(row, good)
    
    def print_row(self, row, good):
        if self.view.show_mode == self.view.SHORT:
            type_cell = row.cell(wrap=False, width=2)
            code_cell = row.cell(wrap=False, width=10)
            name_cell = row.cell()
            unit_cell = row.cell()
            time_cell = row.cell(wrap=False)

            type_cell.text(good.t)
            code_cell.text(good.code)
            name_cell.href(good.name, 'pages/good', {'good_id':good.id})
            if good.unit:
                unit_cell.text(good.unit)
            time_cell.text(good.mtime)
        else:
            code_cell = row.cell(wrap=False, width=10)
            name_cell = row.cell()
            action_cell = row.cell(width=2, wrap=False, align='right')

            TextWithComments(code_cell, good.code, [f'{good.t}, #{good.id}'])
            TextWithComments(name_cell, good.name, [f'{good.description}'])
            EditIconButton(action_cell).onclick('pages/good', {'good_id':good.id})
            DeleteIconButton(action_cell).onclick('.delete', {'good_id':good.id})

    def __call__(self):
        Title(self, F'Товары')
        self.print_toolbar()
        self.print_table()
