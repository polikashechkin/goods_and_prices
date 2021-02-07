from domino.core import log
from domino.pages import Page as BasePage
from domino.pages import Title, Table, Rows, Input, DeleteIconButton, TextWithComments, Toolbar, Select, Button, Text, IconButton
from domino.pages import EditIconButton
from domino.pages import FlatButton
#from domino.pages import Chip
from domino.tables.postgres.dictionary import Dictionary
#from domino.tables.postgres.user import User, UserTable

class Page(BasePage):
    ID = 'pages/groups'

    def __init__(self, application, request):
        super().__init__(application, request)
        self.postgres = None

    def delete(self):
        group_id = self.get('group_id')
        self.postgres.query(Dictionary).filter(Dictionary.id == group_id).delete()
        self.postgres.commit()
        Rows(self, 'table').row(group_id)
        
    def print_toolbar(self):
        finder_panel = Toolbar(self, 'finder_panel').mb(0.2)
        btn = Button(finder_panel.item(ml='auto'), 'Создать').style('background-color:green;color:white')
        #btn.onclick('pages/group_create')

    def print_table(self):
        table = Table(self, 'table')
        #table.column().text('#')
        table.column().text('Код')
        table.column().text('Наименование')
        query = self.postgres.query(Dictionary)\
            .filter(Dictionary.CLASS == 'good', Dictionary.TYPE == 'local_group')\
            .order_by(Dictionary.name)
        for group in query:
            row = table.row(group.id)
            self.print_row(row, group)
    
    def print_row(self, row, group):
        row.cell().text(group.code)
        #row.cell().href(group.name, 'pages/group', {'group_id':group.id})
        row.cell().href(group.name, '')
        #DeleteIconButton(row.cell(width=2)).onclick('.delete', {'group_id':group.id})
 
    def __call__(self):
        Title(self, F'Категории')
        self.print_toolbar()
        self.print_table()
