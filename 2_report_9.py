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

today = datetime.date.today()
yesterday = today - datetime.timedelta(days=1)
first = today.replace(day=1)
lastMonth = first - datetime.timedelta(days=1)
first_day_last_month = lastMonth.replace(day=1).isoformat()

day=(DT.today()-datetime.timedelta(days=7)).isoformat()
file = "/home/strateg-ai/order_scripts/xlsx_files/Отчет_№9_Ежемесячный_отчет_по_поставщикам_зарегистрированным_более_одного_раза_с_{}_по_{}.xlsx".format(first_day_last_month, lastMonth.isoformat())

db_connection = create_engine('postgresql://reports:1@localhost:5432/strateg_2') #1@localhost
session_2 = Session(db_connection)

writer = pd.ExcelWriter(file, engine='xlsxwriter', options={'remove_timezone': True})

query = """select agent.name as "Компания", supplier.created_when as "Дата регистрации", agent.inn as "ИНН", agent.kpp as "КПП", supplier_state_event.supplier_state as "Статус ПФ/Живой", employee.surname as "Фамилия создателя", employee.first_name as "Имя создателя", 
employee.mail as "Почта", temp2.name as "Компания создатель", count (distinct order_s.id) as "Кол-во заказов", count (distinct contract.id) as "Кол-во договоров", max(order_s.created_when) as "Дата последнего заказа", registration.registry_stage as "Статус"
from agent 
left join supplier on supplier.id = agent.fk_supplier_id
inner join (select count (distinct id) as inn_count, inn from agent where fk_supplier_id is not null group by agent.inn) temp on temp.inn = agent.inn
left join events on events.event_driven_id = agent.fk_supplier_id 
left join supplier_state_event on supplier_state_event.event_id = events.id 
left join (select events.event_driven_id, event_type, created_when, registry_stage from events inner join register_stages on register_stages.event_id = events.id where event_type = 'REGISTRATION' ) registration on registration.event_driven_id = agent.fk_supplier_id
left join user_s on user_s.user_name = supplier.created_by 
left JOIN employee ON employee.fk_user_id = user_s.id 
left join (select fk_customer_id, fk_supplier_id, name from agent) temp2 on temp2.fk_customer_id = employee.fk_customer_id or temp2.fk_supplier_id = employee.fk_supplier_id
left join contract on contract.fk_supplier_id = agent.fk_supplier_id
left join order_s on order_s.fk_contract_id = contract.id
where events.event_type = 'SUPPLIER_STATE' and (events.event_driven_id, events.created_when) IN (SELECT events.event_driven_id, MAX(events.created_when) FROM events WHERE event_type = 'SUPPLIER_STATE' GROUP BY events.event_driven_id)
and (registration.event_driven_id, registration.created_when) IN (SELECT events.event_driven_id, MAX(events.created_when) FROM events WHERE event_type = 'REGISTRATION' GROUP BY events.event_driven_id)
and temp.inn_count > 1  and temp.inn_count < 10 
and employee.mail NOT IN ('otpcal@gmail.com', 'bujika@list.ru', 'kos2210@bk.ru', 'kb@tbshelp.ru', 'iri-ukho@yandex.ru')
group by agent.inn, agent.kpp, agent.name, supplier_state_event.supplier_state, supplier.created_when, employee.surname, employee.first_name, employee.mail, temp2.name,  registration.registry_stage
order by agent.inn"""

df2 = pd.read_sql_query(query,db_connection)
df2.to_excel(writer, encoding='utf8', sheet_name="Отчет№9", index=False, freeze_panes = (1,1))
worksheet = writer.sheets["Отчет№9"]

for idx, col in enumerate(df2):
    if df2[col].dtype in ('object', 'string_', 'unicode_') and df2[col].name not in ("Дата последнего заказа"):
        max_len = df2[col].map(len).max()+1
    else:
        series = df2[col].name
        max_len = len(str(series))+1
    worksheet.set_column(idx, idx, max_len)

for idx, row in enumerate(df2["Статус"]):
    if row == 'ACTIVE':
        df2.loc[idx, "Статус"] = 'Рабочая'
    elif row == 'BLOCKED':
        df2.loc[idx, "Статус"] = 'Блокирована'
    elif row == 'REGISTERED':
        df2.loc[idx, "Статус"] = 'Зарегистрирована'
    elif row == 'CONFIRMED_BY_ADMIN':
        df2.loc[idx, "Статус"] = 'Подтверждена администрацией'
    elif row == 'CONFIRMED_BY_USER':
        df2.loc[idx, "Статус"] = 'Подтверждена пользователем'
    elif row == 'DISABLED':
        df2.loc[idx, "Статус"] = 'Удалена'

df2.to_excel(writer, encoding='utf8', sheet_name="Отчет№9", index=False, freeze_panes = (1,1))

writer.save()
send_mail(send_from="report@emsoft.ru", send_to=["report@emsoft.ru", "kaloria@mail.ru"], subject="Отчет №9", text=" Ежемесячный отчет по поставщикам зарегистрированным более одного раза c {} по {} \n\nС уважением,\nООО \"Эмсофт\"\n+7 (495) 230-23-48\nreport@emsoft.ru\n".format(first_day_last_month, lastMonth.isoformat()), files=file)


