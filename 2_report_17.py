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
file = "/home/strateg-ai/order_scripts/xlsx_files/Отчет_№17_Ежемесячный_отчет_о_пролонгированных_договорах_на_срок_свыше_года_и_договорах_с_открытой_датой_окончания_от_{}.xlsx".format(today.isoformat())

db_connection = create_engine('postgresql://reports:1@localhost:5432/strateg_2') #1@localhost
session_2 = Session(db_connection)

writer = pd.ExcelWriter(file, engine='xlsxwriter', options={'remove_timezone': True})

query = """select agent.name as "Конрагент", contract.contract_number as "Номер договора", contract.from_date as "Начало договора", contract.to_date as "Окончание договора", contract_event.contract_state as "Статус договора", sups.name as "Поставщик", sups.inn as "ИНН", sups.kpp as "КПП", 
sups.created_when as "Дата регистрации", sups.state as "Статус поставщика", STRING_AGG (sups.product_group , ' / ') as "Категории"
from agent
INNER JOIN contract on contract.fk_customer_id = agent.fk_customer_id
inner join contract_event on contract_event.fk_contract_id = contract.id

INNER JOIN (select supplier.id as id, agent.name as name, agent.inn as inn, agent.kpp as kpp, agent.created_when as created_when, supplier_state_event.supplier_state as state, temp.surname as surname_creator, temp.first_name as first_name_creator, 
    temp.mail as mail_creator, temp2.name as company_creator, product_group.name as product_group
    from supplier 
    inner join agent on agent.fk_supplier_id = supplier.id
    inner join events on events.event_driven_id = agent.fk_supplier_id 
    inner join supplier_state_event on supplier_state_event.event_id = events.id 
    inner join user_s on user_s.user_name = agent.created_by 
    left join supplier_product_groups on supplier_product_groups.fk_supplier_id = supplier.id
    left join product_group on product_group.id = supplier_product_groups.fk_product_group_id
    inner join (select * from employee) temp ON temp.fk_user_id = user_s.id
    inner join (select fk_customer_id, fk_supplier_id, name from agent) temp2 on temp2.fk_customer_id = temp.fk_customer_id or temp2.fk_supplier_id = temp.fk_supplier_id
    where event_type = 'SUPPLIER_STATE' and (events.event_driven_id, events.created_when) IN (SELECT events.event_driven_id, MAX(events.created_when) FROM events WHERE event_type = 'SUPPLIER_STATE' GROUP BY events.event_driven_id)
    group by supplier.id, agent.name, agent.inn, agent.kpp, agent.created_when, supplier_state_event.supplier_state, temp.surname, temp.first_name, 
    temp.mail, temp2.name, product_group.name) 
    sups on sups.id = contract.fk_supplier_id
WHERE agent.fk_customer_id NOT IN (1232844, 1788755, 1784971, 1784576, 1787831, 1793943, 1788809, 1792883, 1787871, 1787947, 8405381) 
and (contract_event.fk_contract_id, contract_event.created_when) IN (select contract_event.fk_contract_id, max(contract_event.created_when) from contract_event group by contract_event.fk_contract_id) 
and contract_event.contract_state != 'DELETED'
and (((contract.to_date::date - contract.from_date::date)::int > 366 and contract.to_date::date > '{}') or contract.to_date is null)
GROUP BY agent.name, sups.name, sups.inn, sups.kpp, sups.created_when, sups.state, contract.from_date, contract.to_date, contract.contract_number, contract_event.contract_state
ORDER BY agent.name""".format(first_day_last_month)

df2 = pd.read_sql_query(query,db_connection)
df2.to_excel(writer, encoding='utf8', sheet_name="Отчет №17", index=False, freeze_panes = (1,1))
worksheet = writer.sheets["Отчет №17"]

for idx, col in enumerate(df2):
    if df2[col].count() > 2 and df2[col][1] is not None and df2[col].dtypes != 'int64' and df2[col].dtypes != 'float64' and df2[col].name not in ("Номер договора", "Окончание договора", "Статус договора"):
        series = df2[col][1]
    else:
        series = df2[col].name
    max_len = len(str(series))+5
    worksheet.set_column(idx, idx, max_len)

for idx, row in enumerate(df2["Статус договора"]):
    if row == 'NEW':
        df2.loc[idx, "Статус договора"] = 'Новый'
    elif row == 'NOT_ACTIVE':
        df2.loc[idx, "Статус договора"] = 'Неактивный'
    elif row == 'ACTIVE':
        df2.loc[idx, "Статус договора"] = 'Активный'
    elif row == 'AGREED':
        df2.loc[idx, "Статус договора"] = 'Согласован'
    elif row == 'REJECTED':
        df2.loc[idx, "Статус договора"] = 'Отклонен'
    elif row == 'DELETED':
        df2.loc[idx, "Статус договора"] = 'Удален'

for idx, row in enumerate(df2["Статус поставщика"]):
    if row == 'OPERATIVE':
        df2.loc[idx, "Статус поставщика"] = 'Рабочий'
    elif row == 'POSTFACTUM':
        df2.loc[idx, "Статус поставщика"] = 'Постфактум'

df2.to_excel(writer, encoding='utf8', sheet_name="Отчет №17", index=False, freeze_panes = (1,1))

writer.save()
send_mail(send_from="report@emsoft.ru", send_to=["report@emsoft.ru", "kaloria@mail.ru"], subject="Отчет №17", text="Отчет с информацией о пролонгированных договорах на срок свыше года и договорах с открытой датой окончания от {} \n\nС уважением,\nООО \"Эмсофт\"\n+7 (495) 230-23-48\nreport@emsoft.ru\n".format(today.isoformat()), files=file)

