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
first = today.replace(day=1)
lastMonth = first - datetime.timedelta(days=1)
first_day_last_month = lastMonth.replace(day=1).isoformat()

#day=(DT.today()-datetime.timedelta(days=7)).isoformat()
file = "/home/strateg-ai/order_scripts/xlsx_files/Отчет_№19_Ежемесячный_отчет_об_удельном_весе_продаж_определенных_поставщиков_в_общем_обороте_сети_и_по_категориям_отдельно_от_{}.xlsx".format(today.isoformat())

db_connection = create_engine('postgresql://reports:1@localhost:5432/strateg_2') #1@localhost
session_2 = Session(db_connection)

writer = pd.ExcelWriter(file, engine='xlsxwriter', options={'remove_timezone': True})

query = """select all_statistic.product_group as "Категория", temp.name as "Поставщик", temp.inn as "ИНН", temp.kpp as "КПП", sup_statistic.count_orders as "Кол-во заказов", sup_statistic.sum_orders as "Сумма заказов", round((sup_statistic.sum_orders/all_statistic.sum_orders)*100, 2) as "%% от Суммы в категории", round((sup_statistic.sum_orders/(select sum(order_item.quantity*order_item.price) from order_item inner join order_s on order_s.id = order_item.fk_order_id 
    inner join contract on contract.id = order_s.fk_contract_id INNER JOIN events ON events.event_driven_id = order_s.id 
    INNER JOIN order_state_event ON order_state_event.event_id = events.id where contract.fk_customer_id NOT IN (7601315, 1232844, 1788755, 1784971, 1784576, 1787831, 1793943, 1788809, 1792883, 1787871, 1787947, 8405381) and (order_item.price != 0 or order_item.quantity != 0)
    and (events.event_driven_id, events.created_when) IN (SELECT events.event_driven_id, MAX(events.created_when) FROM events WHERE event_type = 'ORDER_STATE' GROUP BY events.event_driven_id) and order_state_event.order_state != 'ORDER_CANCELED'))*100, 2) as "%% от Общей суммы"
from contract
inner join (select * from agent) temp on temp.fk_supplier_id = contract.fk_supplier_id

inner join (select count (distinct order_s.id) as count_orders, sum(order_item.quantity*order_item.price) as sum_orders, contract.fk_supplier_id as id_supplier, product_group.name as product_group
    from order_s
    inner join order_item on order_item.fk_order_id = order_s.id 
    inner join contract on contract.id = order_s.fk_contract_id
    INNER JOIN events ON events.event_driven_id = order_s.id 
    INNER JOIN order_state_event ON order_state_event.event_id = events.id
    left join suppliers_product on suppliers_product.id = order_item.fk_supplier_product_id 
    left join product_name on product_name.id = suppliers_product.fk_product_name_id
    left join product on product.id = product_name.fk_product_id 
    left join product_group on product_group.id = product.fk_product_group_id
    where (events.event_driven_id, events.created_when) IN (SELECT events.event_driven_id, MAX(events.created_when) FROM events WHERE event_type = 'ORDER_STATE' GROUP BY events.event_driven_id) and order_state_event.order_state != 'ORDER_CANCELED'
    and contract.fk_customer_id NOT IN (7601315, 1232844, 1788755, 1784971, 1784576, 1787831, 1793943, 1788809, 1792883, 1787871, 1787947, 8405381) and (order_item.price != 0 or order_item.quantity != 0)
group by contract.fk_supplier_id, product_group.name ) sup_statistic on sup_statistic.id_supplier = temp.fk_supplier_id

inner join (select count (distinct order_s.id) as count_orders, sum(order_item.quantity*order_item.price) as sum_orders, product_group.name  as product_group
    from order_s
    inner join order_item on order_item.fk_order_id = order_s.id 
    inner join contract on contract.id = order_s.fk_contract_id
    INNER JOIN events ON events.event_driven_id = order_s.id 
    INNER JOIN order_state_event ON order_state_event.event_id = events.id
    left join suppliers_product on suppliers_product.id = order_item.fk_supplier_product_id 
    left join product_name on product_name.id = suppliers_product.fk_product_name_id
    left join product on product.id = product_name.fk_product_id 
    left join product_group on product_group.id = product.fk_product_group_id
    where (events.event_driven_id, events.created_when) IN (SELECT events.event_driven_id, MAX(events.created_when) FROM events WHERE event_type = 'ORDER_STATE' GROUP BY events.event_driven_id) and order_state_event.order_state != 'ORDER_CANCELED'
    and contract.fk_customer_id NOT IN (7601315, 1232844, 1788755, 1784971, 1784576, 1787831, 1793943, 1788809, 1792883, 1787871, 1787947, 8405381) and (order_item.price != 0 or order_item.quantity != 0)
group by product_group.name) all_statistic on all_statistic.product_group = sup_statistic.product_group or (all_statistic.product_group is null and sup_statistic.product_group is null)

where contract.fk_customer_id NOT IN (7601315, 1232844, 1788755, 1784971, 1784576, 1787831, 1793943, 1788809, 1792883, 1787871, 1787947, 8405381)
group by temp.name,  sup_statistic.count_orders, sup_statistic.sum_orders, all_statistic.sum_orders, temp.inn, temp.kpp,  all_statistic.product_group
order by all_statistic.product_group, (sup_statistic.sum_orders/all_statistic.sum_orders)*100 DESC"""

df2 = pd.read_sql_query(query,db_connection)
df2.to_excel(writer, encoding='utf8', sheet_name="Отчет №19", index=False, freeze_panes = (1,1))
worksheet = writer.sheets["Отчет №19"]

for idx, col in enumerate(df2):
    if df2[col].dtype in ('object', 'string_', 'unicode_') and df2[col].name not in ('КПП', 'Категория'):
        max_len = df2[col].map(len).max()+1
    else:
        series = df2[col].name
        max_len = 30
    worksheet.set_column(idx, idx, max_len)

df2.to_excel(writer, encoding='utf8', sheet_name="Отчет №19", index=False, freeze_panes = (1,1))

writer.save()
send_mail(send_from="report@emsoft.ru", send_to=["report@emsoft.ru", "kaloria@mail.ru"], subject="Отчет №19", text="Ежемесячный отчет об удельном весе продаж определенных поставщиков в общем обороте сети и по категориям отдельно от {} \n\nС уважением,\nООО \"Эмсофт\"\n+7 (495) 230-23-48\nreport@emsoft.ru\n".format(today.isoformat()), files=file)

