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
yesterday = today - datetime.timedelta(days=1)
#first = today.replace(day=1)
#lastMonth = first - datetime.timedelta(days=1)
#first_day_last_month = lastMonth.replace(day=1).isoformat()

day=(DT.today()-datetime.timedelta(days=7)).isoformat()
file = "/home/strateg-ai/order_scripts/xlsx_files/Отчет_№6_Ежемесячный_накопительный_отчет_по_количеству_и_сумме_заказов_НУЗов_от_.xlsx".format(today.isoformat())

db_connection = create_engine('postgresql://reports:1@localhost:5432/strateg_2') #1@localhost
session_2 = Session(db_connection)

writer = pd.ExcelWriter(file, engine='xlsxwriter', options={'remove_timezone': True})

query = """select
       a.name as "НУЗ",
       sum(sum_item.sum) as "Сумма заказов",
       count(os.id) as "Всего заказов",
       closed.count as "Закрытых заказов",
       cancel.count as "Отмененных заказов",
       count(distinct(c.fk_supplier_id)) as "Поставщики",
       pf.vsego as "Всего заказов ПФ",
       pf.summa as "Сумма заказов ПФ",
       pf.sups as "Поставщики ПФ",
       inn12.vsego as "Всего заказов ИП",
       inn12.summa as "Сумма заказов ИП",
       inn12.sups as "Поставщики ИП"
from order_s os
inner join contract c on c.id = os.fk_contract_id
inner join agent a on a.fk_customer_id = c.fk_customer_id
inner join (select sum(order_item.price*order_item.quantity), order_item.fk_order_id as id from order_item group by fk_order_id) sum_item ON sum_item.id = os.id
left join 
    (select count(distinct os.id), 
            c.fk_customer_id from order_s os 
            inner join events e on e.event_driven_id = os.id 
            inner join order_state_event ose on ose.event_id = e.id 
            inner join 
                (select os.id, 
                        max(e.created_when) as max_date 
                from order_s os 
                inner join events e on e.event_driven_id = os.id 
                group by os.id) 
                order_last_date on order_last_date.id = os.id
            inner join contract c on c.id = os.fk_contract_id 
            where e.created_when = order_last_date.max_date 
            and ose.order_state = 'ORDER_CLOSED' group by c.fk_customer_id)
    CLOSED on closed.fk_customer_id = c.fk_customer_id 
    
left join 
    (select c.fk_customer_id, 
            sum(order_item.price*order_item.quantity) as summa,
            count(distinct(c.fk_supplier_id)) as sups,
            count(distinct os.id) as vsego from order_s os
    inner join contract c on c.id = os.fk_contract_id
    inner join agent a on a.fk_supplier_id = c.fk_supplier_id
    inner join order_item ON order_item.fk_order_id = os.id
    where length(a.inn)=12
    group by c.fk_customer_id) 
    INN12 on inn12.fk_customer_id = c.fk_customer_id
    
left join 
    (select count(distinct order_s.id) as vsego, count(distinct temp.fk_supplier_id) as sups, SUM(order_item.price*order_item.quantity) as summa, contract.fk_customer_id as customer 
    from order_s 
        INNER JOIN contract ON contract.id = order_s.fk_contract_id 
        INNER JOIN supplier ON supplier.id = contract.fk_supplier_id 
        INNER JOIN (SELECT name, inn, fk_supplier_id from agent) temp ON temp.fk_supplier_id = supplier.id 
        INNER JOIN order_item ON order_item.fk_order_id = order_s.id 
        inner join (select * from events) ev_sup on ev_sup.event_driven_id = supplier.id 
        inner join supplier_state_event on supplier_state_event.event_id = ev_sup.id 
    where ev_sup.event_type = 'SUPPLIER_STATE' and supplier_state_event.supplier_state = 'POSTFACTUM' 
    and (ev_sup.event_driven_id, ev_sup.created_when) IN (SELECT events.event_driven_id, MAX(events.created_when) FROM events WHERE event_type = 'SUPPLIER_STATE' GROUP BY events.event_driven_id) 
    GROUP BY customer)
    PF on pf.customer = c.fk_customer_id
left join 
    (select count(distinct os.id), 
            c.fk_customer_id from order_s os 
            inner join events e on e.event_driven_id = os.id 
            inner join order_state_event ose on ose.event_id = e.id 
            inner join 
                (select os.id, 
                        max(e.created_when) as max_date 
                from order_s os 
                inner join events e on e.event_driven_id = os.id 
                group by os.id) 
                order_last_date on order_last_date.id = os.id
            inner join contract c on c.id = os.fk_contract_id 
            where e.created_when = order_last_date.max_date 
            and ose.order_state = 'ORDER_CANCELED' group by c.fk_customer_id)
    CANCEL on cancel.fk_customer_id = c.fk_customer_id
where c.fk_customer_id NOT IN (1232844, 1788755, 1784971, 1784576, 1787831, 1793943, 1788809, 1792883, 1787871, 1787947, 8405381) 
group by a.name, closed.count, cancel.count, inn12.summa, inn12.sups, inn12.vsego, pf.summa, pf.sups, pf.vsego order by a.name"""

df2 = pd.read_sql_query(query,db_connection)
df2.to_excel(writer, encoding='utf8', sheet_name="Отчет№6", index=False, freeze_panes = (1,1))
worksheet = writer.sheets["Отчет№6"]

for idx, col in enumerate(df2):
    if df2[col].dtype in ('object', 'string_', 'unicode_'):
        max_len = df2[col].map(len).max()+1
    else:
        series = df2[col].name
        max_len = len(str(series))+1
    worksheet.set_column(idx, idx, max_len)

df2.to_excel(writer, encoding='utf8', sheet_name="Отчет№6", index=False, freeze_panes = (1,1))

writer.save()
send_mail(send_from="report@emsoft.ru", send_to=["report@emsoft.ru", "kaloria@mail.ru"], subject="Отчет №6", text=" Ежемесячный накопительный отчет по количеству и сумме заказов НУЗов от {} \n\nС уважением,\nООО \"Эмсофт\"\n+7 (495) 230-23-48\nreport@emsoft.ru\n".format(today.isoformat()), files=file)


