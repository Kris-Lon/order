from sqlalchemy import create_engine
from datetime import date as DT
from sqlalchemy.orm import Session
import pandas as pd

# email_imports
from modules.sendmail_excel import send_mail

import datetime
#date_kris = datetime.date(2019, 7, 26)
file = "/home/strateg-ai/order_scripts/xlsx_files/Отчет_№10_Ежемесячный_отчет_по_ДКБ_и_НУЗам_центрального_подчинения_со_статистикой_за_4_последних_недели_от_{}.xlsx".format(DT.today().isoformat())

columns=["Наименование НУЗа"]
db_connection = create_engine('postgresql://reports:1@localhost:5432/strateg_2')
session = Session(db_connection)

dates=[DT.today()]
#dates=[date_kris]
for i in range(0,3):
    dates.append(dates[i] - datetime.timedelta(days=7))
dates.reverse()

# Замудренная генерация запроса
query="SELECT comp.name"
for date in dates:
    query = query + ", count(case when order_s.created_when BETWEEN \'{0} 00:00\' AND \'{1} 00:00\' then 1 else null end)".format((date-datetime.timedelta(days=7)).isoformat(), date.isoformat())
    columns.append("{}.{}-{}.{}".format((date-datetime.timedelta(days=7)).month,(date-datetime.timedelta(days=7)).day,date.month,date.day))
for date in dates:
    query = query + ", sum(case when order_s.created_when BETWEEN \'{0} 00:00\' AND \'{1} 00:00\' then order_sums else null end)".format((date-datetime.timedelta(days=7)).isoformat(), date.isoformat())
    columns.append("{}.{}-{}.{}".format((date-datetime.timedelta(days=7)).month,(date-datetime.timedelta(days=7)).day,date.month,date.day))
for date in dates:
    query = query + ", count(case when event_type = \'SUPPLIER_STATE\' and supplier_state_event.supplier_state = \'POSTFACTUM\' and (events.event_driven_id, events.created_when) IN (SELECT events.event_driven_id, MAX(events.created_when) FROM events WHERE event_type = \'SUPPLIER_STATE\' GROUP BY events.event_driven_id) AND order_s.created_when BETWEEN \'{0} 00:00\' AND \'{1} 00:00\' then 1 else null end)".format((date-datetime.timedelta(days=7)).isoformat(), date.isoformat())
    columns.append("{}.{}-{}.{}".format((date-datetime.timedelta(days=7)).month,(date-datetime.timedelta(days=7)).day,date.month,date.day))
for date in dates:
    query = query + ", sum(case when event_type = \'SUPPLIER_STATE\' and supplier_state_event.supplier_state = \'POSTFACTUM\' and (events.event_driven_id, events.created_when) IN (SELECT events.event_driven_id, MAX(events.created_when) FROM events WHERE event_type = \'SUPPLIER_STATE\' GROUP BY events.event_driven_id) AND order_s.created_when BETWEEN \'{0} 00:00\' AND \'{1} 00:00\' then order_sums else null end)".format((date-datetime.timedelta(days=7)).isoformat(), date.isoformat())
    columns.append("{}.{}-{}.{}".format((date-datetime.timedelta(days=7)).month,(date-datetime.timedelta(days=7)).day,date.month,date.day))
query = query + " from order_s INNER JOIN (SELECT fk_order_id, sum(quantity*price) as order_sums from order_item GROUP by fk_order_id) orders ON orders.fk_order_id=order_s.id INNER JOIN contract ON contract.id = order_s.fk_contract_id INNER JOIN (SELECT name, fk_customer_id FROM agent) comp ON comp.fk_customer_id = contract.fk_customer_id INNER JOIN events ON events.event_driven_id = contract.fk_supplier_id inner join supplier_state_event on supplier_state_event.event_id = events.id where contract.fk_customer_id IN (1784593, 1784596, 1784602, 1784605, 1784608, 1784614, 1784617, 1784644, 1784626, 1784635, 1784632, 1784638, 1784980, 949547, 1784962, 1784956, 1784950, 1784839, 1785001, 1784998, 1785007, 1785010) GROUP BY comp.name"
 
writer = pd.ExcelWriter(file, engine='xlsxwriter')
df = pd.read_sql_query(query,db_connection)
df.columns=columns
new_row=[]
for test in range(len(columns)):
    new_row.append(" ")
df.loc[-1] = new_row  # adding a row
df.index = df.index + 1  # shifting index
df = df.sort_index()
df.to_excel(writer, encoding='utf8', sheet_name="Статистика", index=False)
worksheet = writer.sheets["Статистика"]
worksheet.merge_range('B2:E2', 'Количество заказов')
worksheet.merge_range('F2:I2', 'Сумма заказов')
worksheet.merge_range('J2:M2', 'Количество заказов ПФ')
worksheet.merge_range('N2:Q2', 'Сумма заказов ПФ')
writer.save()
send_mail(send_from="report@emsoft.ru", send_to=["report@emsoft.ru", "kaloria@mail.ru"], subject="Отчет №10", text="Ежемесячный отчет по ДКБ и НУЗам центрального подчинения со статистикой за 4 последних недели от {} \n\nС уважением,\nООО \"Эмсофт\"\n+7 (495) 230-23-48\nreport@emsoft.ru\n".format(DT.today().isoformat()), files=file)
