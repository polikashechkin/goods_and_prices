from domino.core import log
from domino.pages import Page as BasePage
from domino.pages import Title, Table, DeleteIconButton, TextWithComments
from domino.tables.postgres.good_param import GoodParam
from domino.tables.postgres.good import Good
 
class Page(BasePage):
    def __init__(self, application, request):
        super().__init__(application, request)
        self.postgres = None

    def print_table(self):
        table = Table(self, 'table')
        query = self.postgres.query(GoodParam)
        for param in query.order_by(GoodParam.name):
            if param.id in ['local_group', 'unit_id', 'country_id']:
                continue
            row = table.row(param.id)
            self.print_row(row, param)
    
    def print_row(self, row, param):
        
        if param.is_available:
            row.cell().href(param.name, 'pages/good_param', {'column_id':param.column_id})
        else:
            row.cell(style='color:lightgray').text(param.name).tooltip(param.column_id)

    def __call__(self):
        Title(self, 'Дополнительные параметры товара')
        self.print_table()
