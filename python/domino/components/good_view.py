
from domino.tables.postgres.good import Good
from domino.tables.postgres.good_param import GoodParam
from domino.pages import Input, FlatButton, Select, IconButton

class GoodView:
    SHORT = 'short'
    NORMAL = 'normal'

    class DefaultQueryStorage:
        def __init__(self, page):
            self.page = page
        def get(self):
            return self.page.USER_PAGE_STORE.get('query', {})
        def save(self, query):
            return self.page.USER_PAGE_STORE.update({'query':query})

    def __init__(self, page, action, Storage = None):
        self.page = page
        if isinstance(action, str):
            self.action = action
        else:
            self.action = f'.{action.__name__}'
        self.Storage = Storage

        self._query_params = None
        self._params = None
        self._storage = None
        self._show_mode = None

    @property
    def show_mode(self):
        if self._show_mode is None:
            self._show_mode = self.page.USER_PAGE_STORE.get('show_mode', self.NORMAL)
        return self._show_mode

    @show_mode.setter
    def show_mode(self, value):
        self._show_mode = value
        self.page.USER_PAGE_STORE.update({'show_mode':value})
    
    @property
    def query_params(self):
        if self._query_params is None:
            self._query_params = self.storage.get()
        return self._query_params

    @property
    def storage(self):
        if self._storage is None:
            self._storage = self.Storage(self.page) if self.Storage else GoodView.DefaultQueryStorage(self.page)
        return self._storage

    @property
    def params(self):
        if self._params is None:
            self._params = {}
            for param in GoodParam.availables(self.page.postgres):
                self._params[param.column_id] = param
        return self._params

    def print_panel(self, query_panel, finder_panel):
        query_panel.style('flex-wrap: wrap; align-items: flex-end; ')
        finder_panel.style('align-items:center; justify-content: center;')
        forms = [query_panel, finder_panel]
        #--------------------------------------------------------------
        if len(self.query_params):
            for param_id, value in self.query_params.items():
                param = self.params.get(param_id)
                item = query_panel.item(mt=0.5, mr=0.5, mb=0.5)
                g = item.input_group()
                #g.small()
                g.text(param.name)
                #IconButton(g, None, 'check')
                select = g.select(name=param.id, value=value, 
                    options = [['','']] + param.items(self.page.postgres)
                    )
                #select = Select(item, label=param.name, name=param.column_id, value=self.good_query.get(param.column_id))
                select.onchange(self.action, {'action':'query'} , forms=forms)
        #--------------------------------------------------------------
        Input(finder_panel.item(ml=0.1, mb=0.1, mt=0.1, style='width:20rem'), name='finder', placeholder='Поиск..')\
            .onkeypress(13, self.action, {'action':'query'}, forms=forms)
        #--------------------------------------------------------------
        btn = FlatButton(finder_panel.item(), 'Фильтр')
        for param in self.params.values():
            if param.id in self.query_params:
                btn.item(f'- {param.name}')\
                    .onclick(self.action, {'action':'add_or_remove','param_id':param.column_id}, forms=forms)
        for param in self.params.values():
            if param.id not in self.query_params:
                btn.item(f'+ {param.name}').onclick(self.action, {'action':'add_or_remove', 'param_id':param.column_id}, forms=forms)
        #--------------------------------------------------------------
        btn = IconButton(finder_panel.item(), None, 'view_headline')
        if self.show_mode == 'normal':
            btn.onclick(self.action, {'action':'show_mode', 'show_mode':'short'})
        else:
            btn.style('color:lightgray')
            btn.onclick(self.action, {'action':'show_mode', 'show_mode':'normal'})

    def onchange(self):
        action = self.page.get('action')
        if action == 'add_or_remove':
            param_id = self.page.get('param_id')
            if param_id in self.query_params:
                del self.query_params[param_id]
            else:
                self.query_params[param_id] = None
        elif action == 'query':
            for param_id in self.query_params:
                self.query_params[param_id] = self.page.get(param_id)
        elif action=='show_mode':
            self.show_mode = self.page.get('show_mode')
        
        self.storage.save(self.query_params)

    def filter(self, query):
        query = query.filter(Good.filter(self.query_params))
        finder = self.page.get('finder')
        if finder:
            query = query.filter(Good.name.ilike(f'%{finder}%'))
        return query

