import os, sys, sqlite3, json, datetime, arrow
from domino.core import DOMINO_ROOT, log
from domino.page import Page
from domino.page_controls import СтандартныеКнопки, TabControl

class CubeQuery:
    class Column:
        def __init__(self, id, info, is_dim=False, is_exp=False):
            self.id = id
            self.info = info
            self.query = None
            self.is_dim = is_dim
            self.is_exp = is_exp
            self.order_by = None
            self.format = self.info.get('format')
            if self.format is None:
                self.format = '{:,}'
        @property
        def disabled(self):
            return self.query is None
        @disabled.setter
        def disabled(self, value):
            if value:
                self.query = None
            else:
                self.query = ''
        @property
        def name(self):
            return self.info.get('name')
        @property
        def description(self):
            return self.info.get('description')
        @property
        def field(self):
            if self.is_dim:
                return f'{self.id}_id' 
            elif self.is_exp:
                return self.info.get('exp')
            else:
                return self.id
        
        def print_value(self, row, value):
            cell = row.cell()
            cell.style('font-family:monospace; -font-weight: bold; -font-size:1.1em').css('text-right')
            if value:
                if isinstance(value, int):
                    #cell.text(f'{value:,}')    
                    cell.text(self.format.format(value))
                else:
                    value = round(value,2)
                    #cell.text(f'{value:,}')
                    cell.text(self.format.format(value))
            else:
                cell.text('-')

        #def format(self, value):
        #    if value is not None:
        #        f = self.info.get('format')
        #        if f:
        #            return f.format(value)
        #    return value
        @property
        def depend_on(self):
            return self.info.get('depend_on')

    def __init__(self, cube):
        self.cube = cube
        self._columns = {}
        for column_id, info in cube.dims.items():
            self._columns[column_id] = CubeQuery.Column(column_id, info, is_dim=True)
        for column_id, info in cube.values.items():
            self._columns[column_id] = CubeQuery.Column(column_id, info)
        for column_id, info in cube.expressions.items():
            self._columns[column_id] = CubeQuery.Column(column_id, info, is_exp=True)
    
    def load(self, user_id):
        cur = self.cube.connection.cursor()
        cur.execute('select column_id, info from user_profile where user_id = ?', [user_id]) 
        for column_id, INFO in cur:
            c = self._columns.get(column_id)
            if c:
                info = json.loads(INFO)
                c.query = info.get('query')
                c.order_by = info.get('order_by')
    def save(self, user_id):
        with self.cube.connection:
            cur = self.cube.connection.cursor()
            sql = 'insert or replace into user_profile (user_id, column_id, info) values(?,?,?)'
            for c in self._columns.values():
                INFO = json.dumps({'query':c.query, 'order_by':c.order_by})
                cur.execute(sql, [user_id, c.id, INFO])

    def column(self, id):
        return self._columns.get(id)
    def columns(self):
        return self._columns.values()

    def d_columns(self):
        cols = []
        for c in self._columns.values():
            if c.is_dim:
                cols.append(c)
        return cols

    def e_columns(self):
        cols = []
        for c in self._columns.values():
            if c.is_exp:
                cols.append(c)
        return cols

    def show_columns(self):
        cols = []
        for c in self._columns.values():
            if c.disabled:
                continue
            if c.is_dim and c.query.strip() != '': 
                continue
            cols.append(c)
        return cols

    def v_columns(self):
        cols = []
        for c in self._columns.values():
            if not c.is_dim and not c.is_exp:
                cols.append(c)
        return cols

class Cube:
    DIMS = 'dims'
    VALUES = 'values'
    EXPRESSIONS = 'expressions'

    INTEGER = 'integer'
    TEXT = 'text'

    ALL = ''
    UNDEFINED = '?'

    class Dim:
        def __init__(self, id, name, description, **kwargs):
            self.id = id
            self.info = {'name':name, 'description':description}
            for key, value in kwargs.items():
                self.info[key] = value
        def __str__(self):
            return self.id

    class Value:
        def __init__(self, id, name, description, TYPE = None, **kwargs):
            if TYPE is None:
                TYPE = Cube.INTEGER
            self.id = id
            self.info = {'name':name, 'description':description, 'type':TYPE}
            for key, value in kwargs.items():
                self.info[key] = value
        def depend_on(self, *columns):
            depend_on = self.info.get('depend_on')
            if depend_on is None:
                depend_on = []
                self.info['depend_on'] = depend_on
            for column in columns:
                depend_on.append(column.id)
            return self
        def format(self, fmt):
            self.info['format'] = fmt
            return self

        def __str__(self):
            return self.id
    class Expression:
        def __init__(self, id, name, description, TYPE=None, **kwargs):
            if TYPE is None:
                TYPE = Cube.INTEGER
            self.id = id
            self.info = {'name':name, 'description':description, 'type':TYPE}
            for key, value in kwargs.items():
                self.info[key] = value
        def depend_on(self, *columns):
            depend_on = self.info.get('depend_on')
            if depend_on is None:
                depend_on = []
                self.info['depend_on'] = depend_on
            for column in columns:
                depend_on.append(column.id)
            return self
        def expression(self, exp):
            self.info['exp'] = exp
            return self
        def format(self, fmt):
            self.info['format'] = fmt
            return self
        def __str__(self):
            return self.id

    def __init__(self, account_id, module_id, cube_id, name, info = None):
        self.account_id = account_id
        self.module_id = module_id
        self.cube_id = cube_id
        self.name = name
        self._connection = None
        #self.folder = Cube.reports_folder(self.account_id, self.module_id)
        if info is not None:
            self.info = info
        else:
            self.info = {
                'dims' : {},
                'values' : {},
                'expressions' : {}
            }

        self.dims = self.info.get(Cube.DIMS)
        self.__create_dim_names()
        self.values = self.info.get(Cube.VALUES)
        self.expressions = self.info.get(Cube.EXPRESSIONS)

        self.dims_len = len(self.dims) if self.dims is not None else 0
        self.totals = {}

        #log.debug(f'__init__({account_id}, {module_id}, {name}, {info})')
    
    @staticmethod
    def cubes(account_id, module_id):
        class CubeInfo: pass
        cubes = []
        cubes_db = Cube.cubes_db(account_id, module_id)
        exists = os.path.exists(cubes_db)
        #log.debug(f'cubes : {cubes_db} {exists}')
        if not exists:
            return cubes
        with sqlite3.connect(cubes_db) as conn:
            cur = conn.cursor()
            cur.execute('select cube_id, name, modify_time from cubes order by modify_time desc')
            for cube_id, cube_name, modify_time in cur:
                c = CubeInfo()
                c.id = cube_id
                c.name = cube_name
                try:
                    c.modify_time = arrow.get(modify_time)
                except:
                    c.modify_time = None
                cubes.append(c)
            return cubes
        
    @property
    def connection(self):
        if self._connection is None:
            self._connection = self.__connect()
        return self._connection

    def __create_dim_names(self):
        self.dim_names = []
        self.dim_types = []
        for ID in self.dims:
            self.dim_names.append({})
            self.dim_types.append(ID)

    def columns(self, *columns):
        for column in columns:
            if isinstance(column, Cube.Dim):
                self.dims[column.id] = column.info
            elif isinstance(column, Cube.Value):
                self.values[column.id] = column.info
            elif isinstance(column, Cube.Expression):
                self.expressions[column.id] = column.info
        self.__create_dim_names()

    def dim(self, ID, name, **kwargs):
        info = {'type':Cube.TEXT, 'name' : name}
        for key,value in kwargs.items():
            info[key] = value
        self.dims[ID] = info
        self.__create_dim_names()

    def value(self, ID, name, TYPE = None, **kwargs):
        if TYPE is None:
            TYPE = Cube.INTEGER
        info = {'type':TYPE, 'name' : name}
        for key,value in kwargs.items():
            info[key] = value
        self.values[ID] = info
    
    def exp(self, ID, name, TYPE = None, **kwargs): 
        if TYPE is None:
            TYPE = Cube.INTEGER
        info = {'type':TYPE, 'name' : name}
        for key,value in kwargs.items():
            info[key] = value
        self.expressions[ID] = info
    
    @staticmethod
    def cubes_folder(account_id, module_id):
        return os.path.join(DOMINO_ROOT, 'accounts', account_id, 'data', module_id)
    
    @staticmethod
    def cubes_db(account_id, module_id):
        return os.path.join(Cube.cubes_folder(account_id, module_id), 'cubes.db')

    @staticmethod
    def cube_db(account_id, module_id, cube_id):
        return os.path.join(Cube.cubes_folder(account_id, module_id), f'cube_{cube_id}.db')

    def __connect(self):
        cube_db = Cube.cube_db(self.account_id, self.module_id, self.cube_id)
        os.makedirs(os.path.dirname(cube_db), exist_ok=True)
        return sqlite3.connect(cube_db)

    def __repr__(self):
        return f'<Cube {self.cube_id}>'

    def add(self, dim_codes, values, get_name=None):
        #log.debug(f'add({dim_codes}, {values}):')
        totals = self.totals.get(dim_codes)
        if totals is None:
            n = 0
            for code in dim_codes:
                values.append(code)
                if code is not None:
                    names = self.dim_names[n]
                    if code not in names:
                        if get_name is not None:
                            names[code] = get_name(self.dim_types[n], code)
                        else:
                            names[code] = f'{code}'
                n += 1 
            self.totals[dim_codes] = values
        else:
            for i in range(len(values)):
                totals[i] += values[i]
    
    @staticmethod
    def open(account_id, module_id, cube_id):
        cubes_db = Cube.cubes_db(account_id, module_id)
        conn = sqlite3.connect(cubes_db)
        cur = conn.cursor()
        cur.execute('select name, info from cubes where module_id=? and cube_id=?',[module_id, cube_id])
        r = cur.fetchone()
        if r is None:
            return None
        name, INFO = r
        info = json.loads(INFO)
        return Cube(account_id, module_id, cube_id, name, info)

    @staticmethod
    def delete(account_id, module_id, cube_id):
        cubes_db = Cube.cubes_db(account_id, module_id)
        cube_db = Cube.cube_db(account_id, module_id, cube_id)
        if os.path.exists(cube_db):
            os.remove(cube_db)
        with sqlite3.connect(cubes_db) as conn:
            cur = conn.cursor()
            cur.execute('delete from cubes where cube_id=?', [cube_id])

    def create_cube(self):
        cubes_db = Cube.cubes_db(self.account_id, self.module_id)
        os.makedirs(os.path.dirname(cubes_db), exist_ok=True)
        with sqlite3.connect(cubes_db) as conn:
            cur = conn.cursor()
            sql = '''
            create table if not exists cubes (
                module_id text not null,
                cube_id text not null,
                name,
                modify_time,
                state integer,
                info blog,
                primary key (module_id, cube_id)
            )'''
            cur.execute(sql)
            sql = f'insert or replace into cubes (module_id, cube_id, name, modify_time, info) values(?,?,?,?,?)'
            INFO = json.dumps(self.info, ensure_ascii=False)
            params = [self.module_id, self.cube_id, self.name, datetime.datetime.now(), INFO]
            cur.execute(sql, params)
            log.debug(f'{sql} : {params}')

    def save(self, update=False):
        self.create_cube()
        with self.connection:
            cur = self.connection.cursor()
            # создание таблицы           
            sql = []
            sql.append(f'create table if not exists cube (') 
            columns = []
            primary_key = []
            for ID, info in self.dims.items():
                TYPE = info.get('type', Cube.TEXT)
                columns.append(f'{ID}_id {TYPE}')
                primary_key.append(f'{ID}_id')
            for ID, info in self.values.items():
                TYPE = info.get('type', Cube.INTEGER)
                columns.append(f'{ID} {TYPE}')
            #columns.append(f'primary key ({",".join(primary_key)})')
            sql.append(', '.join(columns))
            sql.append(')')
            sql = ' '.join(sql)
            #sql = 'create table if not exists cube'
            log.debug(sql)
            cur.execute(sql)

            # проверка и добавление полей в таблицу
            names = set()
            cur.execute('pragma table_info(cube)')
            for r in cur:
                names.add(r[1])
            log.debug(f'{names}')

            for ID, info in self.dims.items():
                field = f'{ID}_id'
                if field not in names:
                    TYPE = info.get('type', Cube.TEXT)
                    sql = f'alter table cube add column {field} {TYPE}'
                    log.debug(sql)
                    cur.execute(sql)
            for ID, info in self.values.items():
                if ID not in names:
                    TYPE = info.get('type', Cube.INTEGER)
                    sql = f'alter table cube add column {ID} {TYPE}'
                    log.debug(sql)
                    cur.execute(sql)

            # создание профиля пользователя
            sql = '''
            create table if not exists user_profile (
                user_id text,
                column_id text not null,
                info blob default('{}'),
                primary key (user_id, column_id)
            )
            '''
            log.debug(sql)
            cur.execute(sql)

            # удаление всех строк
            if not update:
                sql = 'delete from cube'
                log.debug(sql)
                cur.execute(sql)
            
            # добавление или замещение всех данных
            columns = []
            query = []
            for colname in self.values:
                columns.append(colname)
                query.append('?')
            for colname in self.dims:
                columns.append(f'{colname}_id')
                query.append('?')
            sql = ['insert or replace into cube ']
            #sql.append(self.cube_id)
            sql.append(f' ({",".join(columns)})')
            sql.append(f' values ({",".join(query)})')
            sql = " ".join(sql)

            #print(sql)
            cur.executemany(sql, self.totals.values())

            # добавление (замещение) всех словарей
            log.debug(self.dim_names)
            n = 0
            for ID in self.dims:
                sql = f'create table if not exists "{ID}" (id text primary key, name text)'
                log.debug(sql)
                cur.execute(sql)
                sql = f'insert or replace into "{ID}" (id,name) values(?,?)'
                log.debug(sql)
                cur.executemany(sql, self.dim_names[n].items())
                n += 1

    def query(self):
        return CubeQuery(self)

    def summary(self):
        cur = self.connection.cursor()
        columns = []
        values_fields = {}
        fields = []

        for column_id, info in self.dims.items():
            column = CubeQuery.Column(column_id, info, is_dim=True)
            cur.execute(f'select count(*) from "{column_id}"')
            column.value = cur.fetchone()[0]
            columns.append(column)

        for column_id, info in self.values.items():
            column = CubeQuery.Column(column_id, info)
            column.value = None
            columns.append(column)
            field = f'sum({column_id})'
            values_fields[column_id] = field
            fields.append(field)

        for column_id, info in self.expressions.items():
            column = CubeQuery.Column(column_id, info, is_exp=True)
            column.value = None
            columns.append(column)
            field = column.field.format(**values_fields)
            #for value_id, value_field in values_fields.items():
            #    field = field.replace(value_id, value_field)
            fields.append(field)
 
        sql = f'select {",".join(fields)} from cube limit 1'
        log.debug(f'{sql}')
        log.debug(f'{columns}')
        cur.execute(sql)
        record = cur.fetchone()
        n = 0
        for column in columns:
            if not column.is_dim:
                column.value = record[n]
                n += 1
        return columns

    def select(self, query, limit = None):
        
        fields = []
        values_fields = {}
        join = []
        where = []
        params = []
        group_by = []
        order_by = []

        make_totals = False
        for c in query.d_columns():
            if c.disabled:
                make_totals = True
                continue
            if c.query.strip():
                where.append(f'{c.field} = ?')
                params.append(c.query)
                make_totals = True
            else:
                fields.append(f'"{c.id}".name')
                group_by.append(c.field)
                join.append(f'left join "{c.id}" on "{c.id}".id = {c.field}')
                if c.order_by is not None:
                    if c.order_by:
                        order_by.append(f'"{c.id}".name asc')
                    else:
                        order_by.append(f'"{c.id}".name desc')

        for c in query.v_columns():
            if make_totals:
                if not c.disabled:
                    fields.append(f'sum({c.field})')
                values_fields[c.id] = f'sum({c.field})'
            else:
                if not c.disabled:
                    fields.append(c.field)
                values_fields[c.id] = c.field

        for c in query.e_columns():
            if c.disabled:
                continue
            log.debug(f'{c.field} : {values_fields}')
            field = c.field.format(**values_fields)
            #if make_totals:
            #    for vc in query.v_columns():
            #        field = field.replace(vc.name, f'sum({vc.name})')
            fields.append(field)

        sql = ['select']
        sql.append(','.join(fields))
        sql.append(f'from cube')
        sql.append(' '.join(join))
        if len(where):
            sql.append(f'where {" and ".join(where)}')
        if len(group_by):
            sql.append(f'group by {",".join(group_by)}')
        if limit:
            sql.append(f'limit {limit}')
        sql = " ".join(sql)

        cur = self.connection.cursor()
        log.debug(f'{sql}')
        log.debug(f'{params}')
        cur.execute(sql, params)
        return cur.fetchall()

    def names(self, dim_id):
        cur = self.connection.cursor()
        sql = f'select id, name from "{dim_id}"'
        cur.execute(sql)
        return cur.fetchall()

class CubesPage(Page):
    def __init__(self, application, request, module = None):
        self.module_id = module if module else application.product_id
        super().__init__(application, request)
        #self.SYSADMIN = hasattr(self, 'grants') and self.grants is not None and 1 in self.grants
        self.SYSADMIN = True

    def delete(self):
        cube_id = self.get('cube_id')
        Cube.delete(self.account_id, self.module_id, cube_id)
        self.Row('table', cube_id)

    def open(self):
        self.title(f'Отчеты')

        table = self.Table('table')
        table.column().text('Наименование')
        table.column().text('Последнее изменение')
        for cube in Cube.cubes(self.account_id, self.module_id):
            row = table.row(cube.id)
            row.cell().href(f'{cube.name}', 'cube', {'cube_id' : f'{cube.id}'})
            cell = row.cell()
            if cube.modify_time is not None:
                     cell.text(cube.modify_time.format('YYYY-M-D HH:mm'))
            row.cell()

            кнопки = СтандартныеКнопки(row)
            if self.SYSADMIN:
                кнопки.кнопка('удалить', 'delete', {'cube_id':F'{cube.id}'})

CubePageTabs = TabControl('cube_page_tabs')
CubePageTabs.item('print_totals_tab', 'Описание', 'print_totals_tab')
CubePageTabs.item('print_slices_tab', 'Таблица', 'print_slices_tab')

class CubePage(Page):
    def __init__(self, application, request, module = None):
        self.module_id = module if module else application.product_id
        super().__init__(application, request, controls=[CubePageTabs])
        self.cube_id = self.attribute('cube_id')
        self.cube = Cube.open(self.account_id, self.module_id, self.cube_id)
        self.query = self.cube.query()
        self.query.load(self.user_id)
        log.debug(f'{self.cube.info}')
        self._summary = None
        #self.SYSADMIN = self.grants is not None and 1 in self.grants
        self.SYSADMIN = True
    
    @property
    def summary(self):
        if self._summary is None:
            self._summary = self.cube.summary()
        return self._summary

    def __call__(self):
        self.title(self.cube.name)
        CubePageTabs(self)

    def on_change_disabled(self):
        column_id = self.get('column_id')
        c = self.query.column(column_id)
        c.disabled = not c.disabled
        self.query.save(self.user_id)
        self.print_totals_tab()

    def on_change_order_by(self):
        column_id = self.get('column_id')
        c = self.query.column(column_id)
        if c.order_by is None:
            c.order_by = True
        elif c.order_by:
            c.order_by = False
        else:
            c.order_by = None
        self.query.save(self.user_id)
        self.print_totals_tab()

    def print_totals_tab(self):
        self.Toolbar('toolbar')
        table = self.Table('table').mt(1)
        table1 = self.Table('table1').mt(1)
    
        for column in self.summary:
            user_column = self.query.column(column.id)
            if column.is_dim:
                row = table.row(column.id)
                depend_on = None
                depend_on_columns = None
            else:
                row = table1.row(column.id)
                depend_on = user_column.depend_on
                depend_on_columns = None
                if depend_on is not None:
                    depend_on_columns = []
                    for depend_on_column_id in depend_on:
                        depend_on_column = self.query.column(depend_on_column_id)
                        if depend_on_column.disabled and not user_column.disabled:
                            user_column.disabled = True
                            self.query.save(self.user_id)
                        depend_on_columns.append(depend_on_column)

            #---------------------------
            cell = row.cell(width=2)
            user_column = self.query.column(column.id)
            button = cell.icon_button('check')
            if user_column.disabled:
                button.style('color:lightgray')
            else:
                button.style('color:green')
            button.onclick('.on_change_disabled', {'column_id':column.id})
            #---------------------------
            cell = row.cell(width=2)
            if user_column.order_by is None:
                button = cell.icon_button('arrow_downward')
                button.style('color:lightgray')
                button.tooltip('Сортировка не определена')
            elif user_column.order_by:
                button = cell.icon_button('arrow_downward')
                button.style('color:green')
                button.tooltip('Сортировка по уюыванию (сначала больше, потом меньше)')
            else:
                button = cell.icon_button('arrow_upward')
                button.style('color:green')
                button.tooltip('Сортировка по возрастанию (сначала меньшне потом больше)')
            button.onclick('.on_change_order_by', {'column_id':column.id})
            #---------------------------
            row.cell().text(column.name)
            #---------------------------
            description = column.description if column.description else ''
            if depend_on_columns is not None:
                depend_on_names = []
                for depend_on_column in depend_on_columns:
                    depend_on_names.append(depend_on_column.name) 
                description += f' ({",".join(depend_on_names)})'
            row.cell().text(description)
            #---------------------------
            column.print_value(row, column.value)

    def print_slices_tab(self):
        log.debug(f'print_slices_tab')
        self.print_toolbar()
        self.print_table()
        self.Table('table1')

    def on_query(self):
        for c in self.query.d_columns():
            value = self.get(c.id)
            if value == 'TOTAL':
                c.query = None
            else:
                c.query = value
        self.query.save(self.user_id)
        self.print_table()

    def print_toolbar(self):
        toolbar = self.Toolbar('toolbar').mt(1)
        for c in self.query.d_columns():
            if c.query is not None:
                select = toolbar.item(mr=0.5).select(name=c.id, label = c.name, value = c.query)
                select.option('', 'ВСЕ')
                for code, name in self.cube.names(c.id):
                    select.option(code, name)
                select.onchange('.on_query', forms=[toolbar])

    def print_table(self):
        table = self.Table('table').mt(1)
        records = self.cube.select(self.query, limit = 200)
        columns = self.query.show_columns()
        for column in columns:
            if column.is_dim:
                table.column().text(column.name)
            else:
                table.column(css='text-right').text(column.name)
        for record in records:
            row = table.row()
            n = 0
            for column in columns:
                value = record[n]
                if column.is_dim:
                    row.cell().text(value)
                else:
                    column.print_value(row, value)
                n += 1
            #for field in record:
            #    cell = row.cell()
            #    if field:
            #        if isinstance(field, float):
            #            field = round(field,2)
            #            cell.text(field)
            #        else:
            #            cell.text(field)
            #    else:
            #        cell.text('-')


