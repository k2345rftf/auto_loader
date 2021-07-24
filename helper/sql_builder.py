class Field:

    __slots__ = ('name', 'type_', 'nullable')

    def __init__(self, field_name: str, field_type: str, is_null:bool=True):
        self.name = field_name
        self.type_ = field_type
        self.nullable = 'NULL' if is_null else 'NOT NULL'

    def to_str(self, with_types: bool=True) -> str:
        if with_types:
            return self.name + ' ' + self.type_ + ' ' + self.nullable
        return self.name


class Table:
    __tablename: str
    __columns: dict

    def __init__(self, tablename: str):
        self.__tablename = tablename
        self.__columns = {}

    def add_column(self, columnname: str, type_: str='nvarchar(255)', is_null: bool=True):
        self.__columns[columnname] = Field(columnname, type_, is_null)

    def columns(self, with_types: bool=False):
        return [self.__columns[colname].to_str(with_types) for colname in self.__columns.keys()] 

        
class SqlBuilder:

    __sql: str

    def __init__(self, sql: str=''):
        self.__sql = sql

    def __with_space(self, statement: str):
        string = ' '
        string += statement
        return string

    def select(self, columns: list=[], is_distinct: bool=True, top: int=None):
        self.__sql += self.__with_space('SELECT')
        if is_distinct:
            self.__sql += self.__with_space('DISTINCT')
        if top is not None:
            self.__sql += self.__with_space('TOP' + ' ' + str(top))
        if columns:
            self.__sql += self.__with_space(", ".join(columns))
        else:
            self.__sql += self.__with_space('*')
        return self

    def into(self, tablename: str):
        self.__sql += self.__with_space('INTO' + ' ' + tablename)
        return self

    def from_(self, tablename: str, as_='', with_nolock:bool=True):
        self.__sql += self.__with_space(f"FROM {(tablename + ' ' + as_).strip()}{self.__with_space('(nolock)') if with_nolock else ''}")
        return self

    def where(self):
        if 'WHERE 1=1' not in self.__sql.upper():
            self.__sql += self.__with_space('WHERE 1=1')
        return self

    def condition(self, column: str, value: str, condition: str='=', separator: str='AND'):
        self.__sql += self.__with_space(f'{separator} {column} {condition} {value}')
        return self

    def like(self, column: str, value: str, separator: str='AND', with_not:bool=False):
        self.__sql += self.__with_space(f"{separator} {column} {'NOT' if with_not else ''} LIKE '{value}'")
        return self

    def between(self, column: str, left_value: str, right_value: str, separator: str='AND', with_not:bool=False):
        self.__sql += self.__with_space(f'{separator} {column}{" NOT" if with_not else ""} BETWEEN {left_value} AND {right_value}')
        return self

    def in_(self, column: str, values: list, separator: str='AND', with_not:bool=False):
        prepared_values = ', '.join([str(value) for value in values])
        self.__sql += self.__with_space(f'{separator} {column}{" NOT" if with_not else ""} IN ({prepared_values})')
        return self

    def join(self, tablename: str, how: str='INNER', as_='', with_nolock:bool=True):
        self.__sql += self.__with_space(f"{how} JOIN {(tablename + ' ' + as_).strip()}{self.__with_space('(nolock)') if with_nolock else ''}")
        return self

    def on_(self):
        self.__sql += self.__with_space('ON')
        return self

    def insert_into(self, tablename: str):
        self.__sql += self.__with_space('INSERT INTO' + self.__with_space(tablename))
        return self

    def values(self, columns: list):
        self.__sql += self.__with_space(f'({", ".join([str(col) for col in columns])}) VALUES (' + ', '.join(['?' for _ in columns])+')')
        return self

    def update(self, tablename):
        self.__sql += self.__with_space('UPDATE' + self.__with_space(tablename))
        return self

    def set_(self, columnname: str, value: str):
        sep = ''
        if 'SET' in self.__sql:
            sep = ','
        else:
            self.__sql += self.__with_space('SET')
        self.__sql += sep + self.__with_space(columnname + ' = ' + value)
        return self

    def create_table(self, tablename, columns: list, types: dict={}, not_null_columns: list=[], fg: str='PRIMARY'):
        t = Table(tablename)
        for col in columns:
            nullable_flg = True
            if col in not_null_columns:
                nullable_flg = False
            if col in types:
                t.add_column(col, types[col], is_null=nullable_flg)
            else:
                t.add_column(col, is_null=nullable_flg)
        self.__sql += self.__with_space(f"CREATE TABLE {tablename} ({', '.join(t.columns(True))}) ON {fg}")
        return self

    def compression(self, compression: str='PAGE'):
        self.__sql += self.__with_space(f'WITH (data_compression = {compression})')
        return self

    def sub_query(self):
        return self.__with_space('(' + self.sql() + ')')
    
    def custom_sql(self, sql: str):
        self.__sql = sql

    def show(self):
        return self.sql

    def sql(self):
        sql = self.__sql
        self.__sql = ''
        return sql.strip()

    def copy(self):
        return SqlBuilder(self.__sql)

    
    

        
            
        
        