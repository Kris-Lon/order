# email_imports
from modules.sendmail_excel import send_mail
from modules.email_address import nyz

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
file = "/home/strateg-ai/order_scripts/xlsx_files/Детализация_отчета_по_количестве_договоров_в_которых_присутствует_авансовый_платеж_а_также_с_указанием_%_аванса_от_{}.xlsx".format(today.isoformat())

#nyz = {1784833: ["mik-nuz@mail.ru", "report@emsoft.ru", "lonshakovakristin@gmail.com"], 1784740: ["nuz-secretar@medrzd29.ru", "report@emsoft.ru", "lonshakovakristin@gmail.com"]}

db_connection = create_engine('postgresql://reports:1@localhost:5432/strateg_2') #1@localhost
session_2 = Session(db_connection)

for nyz_i in nyz:
    writer = pd.ExcelWriter(file, engine='xlsxwriter', options={'remove_timezone': True})

    query = """select agent.name as "Имя заказчика", temp.name as "Поставщик", contract.contract_number as "Номер договора", contract.from_date as "Дата с", contract.to_date as "Дата по", contract.payment_type as "Тип оплаты", contract.pre_payment_percent as "Процент предоплаты", STRING_AGG (distinct product_group.name , ' / ') as "Категории"
from contract 
inner join agent on agent.fk_customer_id = contract.fk_customer_id 
inner join (select * from agent) temp on temp.fk_supplier_id = contract.fk_supplier_id
inner join supplier_product_groups on supplier_product_groups.fk_supplier_id = contract.fk_supplier_id
inner join product_group on product_group.id = supplier_product_groups.fk_product_group_id
where contract.payment_type = 'PARTIAL' and contract.fk_customer_id = {} and (contract.to_date >= now() or contract.to_date is null)
group by agent.name, contract.payment_type, contract.pre_payment_percent, contract.contract_number, temp.name, contract.from_date, contract.to_date
order by agent.name,  contract.pre_payment_percent DESC""".format(nyz_i)

    df2 = pd.read_sql_query(query,db_connection)
    df2.to_excel(writer, encoding='utf8', sheet_name="Отчет", index=False, freeze_panes = (1,1))
    worksheet = writer.sheets["Отчет"]
    df = pd.read_sql_query("""select fk_customer_id as "Идентификатор", name as "Имя" from agent where fk_customer_id = {}""".format(nyz_i), db_connection)
    #print(df["Имя"].values[0])
    #print(df2.count())
    for idx, col in enumerate(df2):
        if df2[col].count() > 2 and df2[col][1] is not None and df2[col].dtypes != 'int64' and df2[col].dtypes != 'float64':
            series = df2[col][1]
        else:
            series = df2[col].name
        max_len = len(str(series))+1
        worksheet.set_column(idx, idx, max_len)


    for idx, row in enumerate(df2["Тип оплаты"]):
        if row == 'PARTIAL':
            df2.loc[idx, "Тип оплаты"] = 'Частичная предоплата'
        elif row == 'FULL':
            df2.loc[idx, "Тип оплаты"] = 'Полная'
        elif row == 'POSTPAY':
            df2.loc[idx, "Тип оплаты"] = 'Постоплата'
    df2.to_excel(writer, encoding='utf8', sheet_name="Отчет", index=False, freeze_panes = (1,1))

    writer.save()
    send_mail(send_from="report@emsoft.ru", send_to=nyz[nyz_i], subject="Отчет для {}".format(df["Имя"].values[0]), text="""Детализация отчета по количеству договоров, в которых присутствует авансовый платеж в разрезе категорий, с указанием % аванса от {}. 
В случае если вложенный файл приходит с расширением *.dat, то необходимо переименовать его в *.xlsx.
Также данная неполадка решается путем настройки почтового клиента.

С уважением,
ООО "Эмсофт"
+7 (495) 230-23-48
report@emsoft.ru""".format(today.isoformat()), files=file)

