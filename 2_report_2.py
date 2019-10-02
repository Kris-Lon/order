# email_imports
from modules.sendmail_excel import send_mail

# SQL imports
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
import pandas as pd
import seaborn as sns

# dates import
import datetime
from datetime import date as DT

#date_kris = datetime.date(2019, 6, 28)
#date_kris_2 = date_kris - datetime.timedelta(days=7)

today = datetime.date.today()
first = today.replace(day=1)
lastMonth = first - datetime.timedelta(days=1)
first_day_last_month = lastMonth.replace(day=1).isoformat()

#day=(DT.today()-datetime.timedelta(days=7)).isoformat()
file = "/home/strateg-ai/order_scripts/xlsx_files/Отчет_№2_Ежемесячный_отчет_по_заказам_с_частичной_предоплатой_c_{}_по_{}.xlsx".format(first_day_last_month, lastMonth.isoformat())

db_connection = create_engine('postgresql://reports:1@localhost:5432/strateg_2') #1@localhost
session_2 = Session(db_connection)

writer = pd.ExcelWriter(file, engine='xlsxwriter', options={'remove_timezone': True})

query = """select  distinct on (order_s.order_number, agent.name) order_s.created_when::date as "Дата создания заказа", agent.name as "Имя заказчика", agent.inn as "ИНН заказчика", temp.name as "Имя поставщика", temp.inn as "ИНН поставщика", order_s.order_number as "Номер заказа", contract.contract_number as "Номер договора", count(distinct order_item.id) as "Кол-во позиций в заказе", contract.payment_type as "Тип оплаты", SUM(order_item.price*order_item.quantity) as "Сумма заказа", events_2.pay_sum  as "Оплаченная сумма", events_2.created_when as "Дата изменения суммы", order_state_event.order_state as "Статус заказа", events.created_when as "Дата изменения статуса", contract.pre_payment_percent as "Процент предоплаты", contract.pay_delay_days as "Отсрочка платежа"
from contract 
INNER JOIN order_s ON order_s.fk_contract_id = contract.id
INNER JOIN customer ON customer.id = contract.fk_customer_id 
inner join agent on agent.fk_customer_id = customer.id 
INNER JOIN supplier ON supplier.id = contract.fk_supplier_id 
INNER JOIN (SELECT name, inn, fk_supplier_id from agent) temp ON temp.fk_supplier_id = supplier.id 
INNER JOIN order_item ON order_item.fk_order_id = order_s.id 
INNER JOIN events ON events.event_driven_id = order_s.id 
INNER JOIN order_state_event ON order_state_event.event_id = events.id
LEFT JOIN (select sum(replace(replace(order_param_change_event.new_value, chr(160), ''), ',', '.')::real) as pay_sum, max(events.created_when) as created_when, events.event_driven_id 
    from events 
    LEFT JOIN order_param_change_event on order_param_change_event.event_id = events.id 
    where order_param_change_event.parameter_name = 'customerPaymentSum' and event_type = 'ORDER_PARAM_CHANGE' 
    group by events.event_driven_id) events_2 on events_2.event_driven_id = order_s.id

where customer.id NOT IN (7601315, 1232844, 1788755, 1784971, 1784576, 1787831, 1793943, 1788809, 1792883, 1787871, 1787947, 8405381) 
and (events.event_driven_id, events.created_when) IN (SELECT events.event_driven_id, MAX(events.created_when) FROM events WHERE event_type = 'ORDER_STATE' GROUP BY events.event_driven_id) 
AND contract.payment_type = 'PARTIAL'
and contract.pre_payment_percent is not null 
AND order_s.created_when::date >= '{}' AND order_s.created_when::date <= '{}'
GROUP BY agent.name, order_s.order_number, order_s.created_when, temp.name, temp.inn, order_state_event.order_state, contract.pre_payment_percent, agent.inn, contract.contract_number, contract.payment_type, order_s.customer_payment_sum, events.created_when, events_2.pay_sum, events_2.created_when, 
contract.pay_delay_days
ORDER BY order_s.order_number""".format(first_day_last_month, lastMonth.isoformat())

df2 = pd.read_sql_query(query,db_connection)
df2.to_excel(writer, encoding='utf8', sheet_name="Отчет№2", index=False, freeze_panes = (1,1))
worksheet = writer.sheets["Отчет№2"]

for idx, col in enumerate(df2):
    if df2[col].dtype in ('object', 'string_', 'unicode_') and df2[col].name not in ('Дата создания заказа', 'Дата изменения суммы'):
        max_len = df2[col].map(len).max()+1
    else:
        series = df2[col].name
        max_len = len(str(series))+1
    worksheet.set_column(idx, idx, max_len)

for idx, row in enumerate(df2["Тип оплаты"]):
    if row == 'FULL':
        df2.loc[idx, "Тип оплаты"] = 'Полная'
    elif row == 'PARTIAL':
        df2.loc[idx, "Тип оплаты"] = 'Частичная'
    elif row == 'POSTPAY':
        df2.loc[idx, "Тип оплаты"] = 'Постоплата'

for idx, row in enumerate(df2["Статус заказа"]):
    if row == 'NEW':
        df2.loc[idx, "Статус заказа"] = 'Новый'
    elif row == 'EXECUTION':
        df2.loc[idx, "Статус заказа"] = 'Выполнение'
    elif row == 'DOCUMENTS_POSTFACTUM':
        df2.loc[idx, "Статус заказа"] = 'Документы (постфактум)'
    elif row == 'ORDER_CLOSED':
        df2.loc[idx, "Статус заказа"] = 'Заказ закрыт'
    elif row == 'ORDER_CANCELED':
        df2.loc[idx, "Статус заказа"] = 'Заказ отменен'
    elif row == 'PAYMENT':
        df2.loc[idx, "Статус заказа"] = 'Оплата'
    elif row == 'PAYMENT_RECEIVED':
        df2.loc[idx, "Статус заказа"] = 'Поступление ДС'
    elif row == 'PARTIAL_POST_PAYMENT_RECEIVED':
        df2.loc[idx, "Статус заказа"] = 'Поступление ДС постоплаты'
    elif row == 'PARTIAL_PRE_PAYMENT_RECEIVED':
        df2.loc[idx, "Статус заказа"] = 'Поступление ДС предоплаты'
    elif row == 'AGREED_BY_SUPPLIER':
        df2.loc[idx, "Статус заказа"] = 'Согласование поставщиком'
    elif row == 'PARTIAL_POST_PAYMENT':
        df2.loc[idx, "Статус заказа"] = 'Частичная постоплата'
    elif row == 'PARTIAL_PRE_PAYMENT':
        df2.loc[idx, "Статус заказа"] = 'Частичная предоплата'
    elif row == 'RECEPTION':
        df2.loc[idx, "Статус заказа"] = 'Получение'
    elif row == 'ORDER_RESULTS':
        df2.loc[idx, "Статус заказа"] = 'Редактирование заказа'
    elif row == 'ORDER_CLOSED_POSTFACTUM':
        df2.loc[idx, "Статус заказа"] = 'Заказ закрыт (постфактум)'
    elif row == 'PAYMENT_POSTFACTUM':
        df2.loc[idx, "Статус заказа"] = 'Оплата (постфактум)'
    elif row == 'ORDER_RESULTS_POSTFACTUM':
        df2.loc[idx, "Статус заказа"] = 'Редактирование заказа (постфактум)'
#for idx, row in enumerate(df2["НДС"]):
#    if row == -1:
#        df2.loc[idx, "НДС"] = 'Без НДС'
df2.to_excel(writer, encoding='utf8', sheet_name="Отчет№2", index=False, freeze_panes = (1,1))

writer.save()
send_mail(send_from="report@emsoft.ru", send_to=["report@emsoft.ru", "kaloria@mail.ru"], subject="Отчет №2", text=" Ежемесячный отчет по заказам с частичной предоплатой c {} по {} \n\nС уважением,\nООО \"Эмсофт\"\n+7 (495) 230-23-48\nreport@emsoft.ru\n".format(first_day_last_month, lastMonth.isoformat()), files=file)


