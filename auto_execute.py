
# coding: utf-8

# In[ ]:

from helper.sql_context import SqlContext, SqlGenerator, DateGenerator
from helper.sql_builder import SqlBuilder
from databases.Database import DatabaseFactory
from tqdm import tqdm
import time


# In[ ]:

db = DatabaseFactory().build('*наименование сервера*')


# 

# In[ ]:

db.collect('select top 100 * from table')
db.execute('insert into table select * from another_table')
db.execute_many ('insert into from table (id, name, age) values (?,?,?)', [1,’Jhon’, 25])



# 

# In[ ]:

date_gen = DateGenerator('2021-01-01', '2021-05-31')
date_gen.setup_periods(days=5)
dates = list(date_gen.generate())


# 



# In[ ]:

 context = SqlContext()
context.add_var('col_name’, [1,2,3,4,5], separator='AND', condition='=')
context.add_var('col_name_1’, [[‘a’,’b’,’v’], [‘a1’,’b2’,’v3’],] , separator='AND', condition='in')



# In[ ]:

context.__dict__


# 

# In[ ]:

builder = SqlBuilder()
builder.custom_sql('''
INSERT INTO insertable_table
SELECT
*
FROM table
WHERE 1=1
AND col1 in (1, 2,10,98,34)
AND col2 = 9
AND col3 between ‘20200101’ and ‘20200201’
''')



# In[ ]:

builder.sql()


# In[ ]:

generator = SqlGenerator(builder, context)


# In[ ]:

for sql in tqdm(generator.generate()):
    t = time.time()
    db.execute(sql)
    print('Итоговое время работы запроса: ' + str(time.time()-t))
