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
file = "/home/strateg-ai/order_scripts/xlsx_files/Ежемесячный_отчет_о_цикличности_поставок_и_сроки_между_ними_по_всем_товарам_работам_и_услугам_за_1_месяц_от_{}.xlsx".format(today.isoformat())

#nyz = {1784833: ["mik-nuz@mail.ru", "report@emsoft.ru", "lonshakovakristin@gmail.com"], 1784740: ["nuz-secretar@medrzd29.ru", "report@emsoft.ru", "lonshakovakristin@gmail.com"]}

db_connection = create_engine('postgresql://reports:1@localhost:5432/strateg_2') #1@localhost
session_2 = Session(db_connection)

for nyz_i in nyz:
    writer = pd.ExcelWriter(file, engine='xlsxwriter', options={'remove_timezone': True})

    query = """select agent.name as "Заказчик", temp.name as "Поставщик", order_item.product_name as "Товар",  product_group.name as "Категория", MIN(order_s.created_when) as "Дата первого заказа",  MAX(order_s.created_when) as "Дата последнего заказа", count(distinct order_s.id) as "Кол-во заказов", (MAX(order_s.created_when) - MIN(order_s.created_when))/count(distinct order_s.id) as "Цикличность ",   round(avg(order_item.price), 2) as "Средная цена на товар", round(avg(order_item.quantity), 2) as "Среднее кол-во товара", 
round(avg(order_item.quantity*order_item.price), 2) as "Средняя сумма заказов", sum(order_item.quantity) as "Общее кол-во товара", round(sum(order_item.quantity*order_item.price), 2) as "Общая сумма заказов", round(max(order_item.price), 2) as "MAX цена на товар", round(max(order_item.quantity), 2) as "MAX кол-во товара", 
round(max(order_item.quantity*order_item.price), 2) as "MAX сумма заказов", round(min(order_item.price), 2) as "MIN цена на товар", round(min(order_item.quantity), 2) as "MIN кол-во товара", round(min(order_item.quantity*order_item.price), 2) as "MIN сумма заказов"
from order_s 
inner join contract on contract.id = order_s.fk_contract_id
inner join agent on agent.fk_customer_id = contract.fk_customer_id
inner join (select * from agent) temp on temp.fk_supplier_id = contract.fk_supplier_id
inner join order_item on order_item.fk_order_id = order_s.id
INNER JOIN events ON events.event_driven_id = order_s.id 
INNER JOIN order_state_event ON order_state_event.event_id = events.id
left join suppliers_product on suppliers_product.id = order_item.fk_supplier_product_id 
left join hb_unit on hb_unit.id = suppliers_product.fk_unit_id
left join product_name on product_name.id = suppliers_product.fk_product_name_id
left join product on product.id = product_name.fk_product_id 
left join product_group on product_group.id = product.fk_product_group_id

WHERE order_item.quantity != 0 and order_item.price != 0 and (events.event_driven_id, events.created_when) IN (SELECT events.event_driven_id, MAX(events.created_when) FROM events WHERE event_type = 'ORDER_STATE' GROUP BY events.event_driven_id) 
and order_state_event.order_state != 'ORDER_CANCEL' and contract.fk_customer_id = {}
AND order_s.created_when::date >= '{}'
GROUP BY order_item.product_name, agent.name, temp.name, product_group.name
HAVING count(distinct order_s.id) > 1
ORDER BY agent.name, (MAX(order_s.created_when) - MIN(order_s.created_when))/count(distinct order_s.id) DESC""".format(nyz_i, first_day_last_month)

    df2 = pd.read_sql_query(query,db_connection)
    df2.to_excel(writer, encoding='utf8', sheet_name="Отчет №18", index=False, freeze_panes = (1,1))
    worksheet = writer.sheets["Отчет №18"]
    df = pd.read_sql_query("""select fk_customer_id as "Идентификатор", name as "Имя" from agent where fk_customer_id = {}""".format(nyz_i), db_connection)
    for idx, col in enumerate(df2):
        if df2[col].count() > 2 and df2[col][1] is not None and df2[col].dtypes != 'int64' and df2[col].dtypes != 'float64' and df2[col].name not in ("Номер договора", "Окончание договора", "Статус договора"):
            series = df2[col][1]
        else:
            series = df2[col].name
        max_len = len(str(series))+5
        worksheet.set_column(idx, idx, max_len)

    df2.to_excel(writer, encoding='utf8', sheet_name="Отчет №18", index=False, freeze_panes = (1,1))

    writer.save()
    send_mail(send_from="report@emsoft.ru", send_to=nyz[nyz_i], subject="Отчет для {}".format(df["Имя"].values[0]), text="""Ежемесячный отчёт о цикличности поставок и сроки между ними по всем товарам, работам и услугам за 1 месяц от {}. 
В случае если вложенный файл приходит с расширением *.dat, то необходимо переименовать его в *.xlsx.
Также данная неполадка решается путем настройки почтового клиента.

С уважением,
ООО "Эмсофт"
+7 (495) 230-23-48
report@emsoft.ru""".format(today.isoformat()), files=file)

