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
file = "/home/strateg-ai/order_scripts/xlsx_files/Отчет_№16_Ежемесячный_отчет_с_информацией_по_незакрытым_заказам_от_{}.xlsx".format(today.isoformat())

db_connection = create_engine('postgresql://reports:1@localhost:5432/strateg_2') #1@localhost
session_2 = Session(db_connection)

writer = pd.ExcelWriter(file, engine='xlsxwriter', options={'remove_timezone': True})

query = """select
       a.name as "НУЗ",
       count(os.id) as "Всего заказов",
       round(sum(sum_item.sum), 2) as "Сумма заказов",
       count(distinct(c.fk_supplier_id)) as "Поставщиков",
       pf.vsego as "Всего заказов ПФ",
       round(pf.summa, 2) as "Сумма заказов ПФ",
       pf.sups as "Поставщиков ПФ",
       closed.count as "Не закрытых",
       round(100.0*closed.count/count(os.id), 2) as "Процент не закрытых",
       round(closed.summa,2) as "Сумма по не закрытым",
       PF_not_close.vsego as "ПФ не закрытых",
       round(100.0*PF_not_close.vsego/pf.vsego, 2) as "Процент не закрытых ПФ",
       round(PF_not_close.summa, 2) as "Сумма по не закрытым ПФ"
from order_s os
inner join contract c on c.id = os.fk_contract_id
inner join agent a on a.fk_customer_id = c.fk_customer_id
inner join (select sum(order_item.price*order_item.quantity), order_item.fk_order_id as id from order_item group by fk_order_id) sum_item ON sum_item.id = os.id
left join 
    (select count(distinct os.id), 
            c.fk_customer_id, sum(order_item.price*order_item.quantity) as summa from order_s os 
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
            inner join order_item ON order_item.fk_order_id = os.id
            where e.created_when = order_last_date.max_date 
            and ose.order_state != 'ORDER_CLOSED' and ose.order_state != 'ORDER_CANCELED' group by c.fk_customer_id)
    CLOSED on closed.fk_customer_id = c.fk_customer_id 
    
left join 
    (select count(distinct order_s.id) as vsego, contract.fk_customer_id as customer, SUM(order_item.price*order_item.quantity) as summa, count(distinct contract.fk_supplier_id) as sups 
        from order_s 
        INNER JOIN contract ON contract.id = order_s.fk_contract_id  
        INNER JOIN events ON events.event_driven_id = order_s.id 
        INNER JOIN order_state_event ON order_state_event.event_id = events.id 
        INNER JOIN order_item on order_item.fk_order_id =order_s.id 
        INNER JOIN (select os.id, max(e.created_when) as max_date from order_s os inner join events e on e.event_driven_id = os.id where e.event_type = 'ORDER_STATE' group by os.id) order_last_date on order_last_date.id = order_s.id
        where events.created_when = order_last_date.max_date and (order_state_event.order_state = 'DOCUMENTS_POSTFACTUM' or order_state_event.order_state = 'ORDER_CLOSED_POSTFACTUM' or order_state_event.order_state = 'PAYMENT_POSTFACTUM' or order_state_event.order_state = 'ORDER_RESULTS_POSTFACTUM')
        GROUP BY contract.fk_customer_id)
    PF on pf.customer = c.fk_customer_id
    
left join 
    (select count(distinct order_s.id) as vsego, contract.fk_customer_id as customer, SUM(order_item.price*order_item.quantity) as summa, count(distinct contract.fk_supplier_id) as sups 
        from order_s 
        INNER JOIN contract ON contract.id = order_s.fk_contract_id  
        INNER JOIN events ON events.event_driven_id = order_s.id 
        INNER JOIN order_state_event ON order_state_event.event_id = events.id 
        INNER JOIN order_item on order_item.fk_order_id =order_s.id 
        INNER JOIN (select os.id, max(e.created_when) as max_date from order_s os inner join events e on e.event_driven_id = os.id where e.event_type = 'ORDER_STATE' group by os.id) order_last_date on order_last_date.id = order_s.id
        where events.created_when = order_last_date.max_date and (order_state_event.order_state = 'DOCUMENTS_POSTFACTUM' or order_state_event.order_state = 'PAYMENT_POSTFACTUM' or order_state_event.order_state = 'ORDER_RESULTS_POSTFACTUM')
        GROUP BY contract.fk_customer_id)
    PF_not_close on PF_not_close.customer = c.fk_customer_id
where c.fk_customer_id NOT IN (7601315, 1232844, 1788755, 1784971, 1784576, 1787831, 1793943, 1788809, 1792883, 1787871, 1787947, 8405381) 
group by c.fk_customer_id, a.name, closed.count, pf.summa, closed.summa, pf.sups, pf.vsego, PF_not_close.vsego, PF_not_close.summa
order by count(os.id) DESC"""

df2 = pd.read_sql_query(query,db_connection)
df2.to_excel(writer, encoding='utf8', sheet_name="Отчет №16", index=False, freeze_panes = (1,1))
worksheet = writer.sheets["Отчет №16"]

for idx, col in enumerate(df2):
    if df2[col].count() > 2 and df2[col][1] is not None and df2[col].dtypes != 'int64' and df2[col].dtypes != 'float64':
        series = df2[col][1]
    else:
        series = df2[col].name
    max_len = len(str(series))+1
    worksheet.set_column(idx, idx, max_len)

df2.to_excel(writer, encoding='utf8', sheet_name="Отчет №16", index=False, freeze_panes = (1,1))

writer.save()
send_mail(send_from="report@emsoft.ru", send_to=["report@emsoft.ru", "kaloria@mail.ru"], subject="Отчет №16", text="Ежемесячный отчет с информацией по незакрытым заказам от {} \n\nС уважением,\nООО \"Эмсофт\"\n+7 (495) 230-23-48\nreport@emsoft.ru\n".format(today.isoformat()), files=file)

