from helper.sql_builder import SqlBuilder
import itertools
from datetime import datetime, timedelta

class Generator:
    
    def generate(self):
        raise NotImplementedError()



class SqlContext:
    
    def __init__(self, var: dict={}, conditions: dict={}, separators: dict={}):
        self.__var = var
        self.__count_vars = len(self.__var)
        self.__var_conditions = conditions
        self.__var_separator = separators

    def __prepare_value(self, value: str, add_quotes: bool=False):
        if add_quotes:
            value = "'" + str(value) + "'"
        return str(value)
        
    def add_var(self, name: str, values: list, condition: str='=', separator: str='AND', with_quotes: bool=False):
        
        values = tuple(values)
        if isinstance(values[0], list) or isinstance(values[0], tuple):
            self.__var[name]=tuple([[self.__prepare_value(val, with_quotes) for val in value] for value in values])
        else:
            self.__var[name]=tuple([self.__prepare_value(value, with_quotes) for value in values])
        self.__count_vars+=1
        self.__var_conditions[name] = condition.upper()
        self.__var_separator[name]  = separator.upper()
    
    
    def __iter__(self):
        for values in self.unpack():
            yield values   
   
    def conditions(self):
        return self.__var_conditions
    
    def separators(self):
        return self.__var_separator
        
    def keys(self):
        return self.__var.keys()
    
    def unpack(self):
        return itertools.product(*[v for k, v in self.__var.items()])
    
    def copy(self):
        return SqlContext(self.__var.copy(), condition=self.__var_conditions, separator=self.__var_separator)
  
      
class SqlGenerator(Generator):
    
    def __init__(self, sql_builder: SqlBuilder, context: SqlContext):
        self.__builder = sql_builder
        self.__context = context
    
    def generate(self) -> list:
        keys = self.__context.keys()
        conditions = self.__context.conditions()
        separators = self.__context.separators()
        res = []
        for values in self.__context:
            builder = self.__builder.copy()
            builder.where()
            for k, v in zip(keys, values):
                if conditions[k]=='BETWEEN':
                    builder.between(k, v[0], v[1], separator=separators[k])
                elif conditions[k] in ['IN', 'NOT IN']:
                    builder.in_(k, v, separator=separators[k], with_not=conditions[k] ==  'NOT IN') 
                else:
                    builder.condition(k, v, condition=conditions[k], separator=separators[k]) 
            res.append(builder.sql())
        return res

        
class DateGenerator(Generator):

    
    def __init__(self, start_date: str, end_date: str, format='%Y-%m-%d'):
        self.__s_dt = datetime.strptime(start_date, format)
        self.__e_dt = datetime.strptime(end_date, format)
        self.__periods = []
    
    def setup_periods(self, days: int=0, month: int=0, year: int=0, week: int=0):
        days += week*7 + month*4*7 + 12*4*year*7
        delta = timedelta(days=days)
        start = self.__s_dt
        self.__periods = []
        while (self.__e_dt - start)>=delta:
            self.__periods.append((start, start+delta))
            start = start + delta + timedelta(days=1)
        if start != self.__e_dt:
            self.__periods.append((start, self.__e_dt))
        else:
            self.__periods[-1] = (self.__periods[-1][0], self.__e_dt)
        
    def generate(self, format='%Y%m%d'):
        for beg_td, end_dt in self.__periods:
            yield datetime.strftime(beg_td, format=format), datetime.strftime(end_dt, format=format)
        
          
# if __name__ == '__main__':
#     context = SqlContext()
    
#     context.add_var('dt',[('a','b','v')], condition='in')
#     context.add_var('x',['1','2','3'])
    
#     # dt = DateGenerator('2020-01-01', '2021-01-01')
#     # dt.setup_periods(days=5)
#     # for b_dt, e_dt in dt.generate():
#     #     print(b_dt, e_dt)
#     gen = SqlGenerator(SqlBuilder().select('tablename'), context)
#     for v in gen.generate():
#         print(v)
    
        
