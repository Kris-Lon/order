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
first = today.replace(day=1)
lastMonth = first - datetime.timedelta(days=1)
first_day_last_month = lastMonth.replace(day=1).isoformat()

day=(DT.today()-datetime.timedelta(days=7)).isoformat()
file = "/home/strateg-ai/order_scripts/xlsx_files/Отчет_№12_Еженедельный_накопительный_отчет_по_всем_статусам_заказов_всех_НУЗов_c_{}_по_{}.xlsx".format(first_day_last_month, lastMonth.isoformat())

db_connection = create_engine('postgresql://reports:1@localhost:5432/strateg_2') #1@localhost
session_2 = Session(db_connection)

writer = pd.ExcelWriter(file, engine='xlsxwriter', options={'remove_timezone': True})

query = """select
       a.name as "НУЗ",
       count(os.id) as "Всего заказов",
       sum(sum_item.sum) as "Сумма заказов",
       count(distinct(c.fk_supplier_id)) as "Поставщиков",
       pf.vsego as "Всего заказов ПФ",
       pf.summa as "Сумма заказов ПФ",
       pf.sups as "Поставщиков ПФ",
       closed.count as "Закрытых",
       closed.summa as "Сумма 1",
       payment.count as "Оплаченных",
       payment.summa as "Сумма 2",
       cancel.count as "Отмененных",
       cancel.summa as "Сумма 3",
       execution.count as "На выполнении",
       execution.summa as "Сумма 4",
       new.count as "Новых",
       new.summa as "Сумма 5",
       PAYMENT_RECEIVED.count as "На поступлении ДС",
       PAYMENT_RECEIVED.summa as "Сумма 6",
       RECEPTION.count as "На получении",
       RECEPTION.summa as "Сумма 7",
       ORDER_RESULTS.count as "На редактировании",
       ORDER_RESULTS.summa as "Сумма 8",
       AGREED_BY_SUPPLIER.count as "На согласовании поставщиком",
       AGREED_BY_SUPPLIER.summa as "Сумма 9"
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
            and ose.order_state = 'ORDER_CLOSED' AND os.created_when::date >= '{}' AND os.created_when::date <= '{}' group by c.fk_customer_id)
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
        AND order_s.created_when::date >= '{}' AND order_s.created_when::date <= '{}'
        GROUP BY contract.fk_customer_id)
    PF on pf.customer = c.fk_customer_id
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
            and ose.order_state = 'ORDER_CANCELED' AND os.created_when::date >= '{}' AND os.created_when::date <= '{}' group by c.fk_customer_id)
    CANCEL on cancel.fk_customer_id = c.fk_customer_id
left join 
    (select count(distinct order_s.id) as count, contract.fk_customer_id as customer, SUM(order_item.price*order_item.quantity) as summa 
        from order_s 
        INNER JOIN contract ON contract.id = order_s.fk_contract_id  
        INNER JOIN events ON events.event_driven_id = order_s.id 
        INNER JOIN order_state_event ON order_state_event.event_id = events.id 
        INNER JOIN order_item on order_item.fk_order_id =order_s.id 
        INNER JOIN (select os.id, max(e.created_when) as max_date from order_s os inner join events e on e.event_driven_id = os.id where e.event_type = 'ORDER_STATE' group by os.id) order_last_date on order_last_date.id = order_s.id
        where events.created_when = order_last_date.max_date and (order_state_event.order_state = 'PAYMENT' or order_state_event.order_state = 'PARTIAL_POST_PAYMENT' or order_state_event.order_state = 'PARTIAL_PRE_PAYMENT')
        AND order_s.created_when::date >= '{}' AND order_s.created_when::date <= '{}'
        GROUP BY contract.fk_customer_id)
    payment on payment.customer = c.fk_customer_id
left join
    (select count(distinct order_s.id) as count, contract.fk_customer_id as customer, SUM(order_item.price*order_item.quantity) as summa 
        from order_s 
        INNER JOIN contract ON contract.id = order_s.fk_contract_id  
        INNER JOIN events ON events.event_driven_id = order_s.id 
        INNER JOIN order_state_event ON order_state_event.event_id = events.id 
        INNER JOIN order_item ON order_item.fk_order_id = order_s.id 
        INNER JOIN (select os.id, max(e.created_when) as max_date from order_s os inner join events e on e.event_driven_id = os.id where e.event_type = 'ORDER_STATE' group by os.id) order_last_date on order_last_date.id = order_s.id 
        where events.created_when = order_last_date.max_date and order_state_event.order_state = 'EXECUTION' 
        AND order_s.created_when::date >= '{}' AND order_s.created_when::date <= '{}'
        GROUP BY contract.fk_customer_id)
    EXECUTION on EXECUTION.customer = c.fk_customer_id
left join
    (select count(distinct order_s.id) as count, contract.fk_customer_id as customer, SUM(order_item.price*order_item.quantity) as summa 
        from order_s 
        INNER JOIN contract ON contract.id = order_s.fk_contract_id  
        INNER JOIN events ON events.event_driven_id = order_s.id 
        INNER JOIN order_state_event ON order_state_event.event_id = events.id 
        INNER JOIN order_item ON order_item.fk_order_id = order_s.id 
        INNER JOIN (select os.id, max(e.created_when) as max_date from order_s os inner join events e on e.event_driven_id = os.id where e.event_type = 'ORDER_STATE' group by os.id) order_last_date on order_last_date.id = order_s.id 
        where events.created_when = order_last_date.max_date and order_state_event.order_state = 'NEW' 
        AND order_s.created_when::date >= '{}' AND order_s.created_when::date <= '{}'
        GROUP BY contract.fk_customer_id)
    NEW on NEW.customer = c.fk_customer_id
left join
    (select count(distinct order_s.id) as count, contract.fk_customer_id as customer, SUM(order_item.price*order_item.quantity) as summa 
        from order_s 
        INNER JOIN contract ON contract.id = order_s.fk_contract_id  
        INNER JOIN events ON events.event_driven_id = order_s.id 
        INNER JOIN order_state_event ON order_state_event.event_id = events.id 
        INNER JOIN order_item ON order_item.fk_order_id = order_s.id 
        INNER JOIN (select os.id, max(e.created_when) as max_date from order_s os inner join events e on e.event_driven_id = os.id where e.event_type = 'ORDER_STATE' group by os.id) order_last_date on order_last_date.id = order_s.id 
        where events.created_when = order_last_date.max_date and (order_state_event.order_state = 'PAYMENT_RECEIVED' or  order_state_event.order_state = 'PARTIAL_POST_PAYMENT_RECEIVED' or order_state_event.order_state = 'PARTIAL_PRE_PAYMENT_RECEIVED')
        AND order_s.created_when::date >= '{}' AND order_s.created_when::date <= '{}'
        GROUP BY contract.fk_customer_id)
    PAYMENT_RECEIVED on PAYMENT_RECEIVED.customer = c.fk_customer_id
left join
    (select count(distinct order_s.id) as count, contract.fk_customer_id as customer, SUM(order_item.price*order_item.quantity) as summa 
        from order_s 
        INNER JOIN contract ON contract.id = order_s.fk_contract_id  
        INNER JOIN events ON events.event_driven_id = order_s.id 
        INNER JOIN order_state_event ON order_state_event.event_id = events.id 
        INNER JOIN order_item ON order_item.fk_order_id = order_s.id 
        INNER JOIN (select os.id, max(e.created_when) as max_date from order_s os inner join events e on e.event_driven_id = os.id where e.event_type = 'ORDER_STATE' group by os.id) order_last_date on order_last_date.id = order_s.id 
        where events.created_when = order_last_date.max_date and order_state_event.order_state = 'RECEPTION' 
        AND order_s.created_when::date >= '{}' AND order_s.created_when::date <= '{}'
        GROUP BY contract.fk_customer_id)
    RECEPTION on RECEPTION.customer = c.fk_customer_id
left join
    (select count(distinct order_s.id) as count, contract.fk_customer_id as customer, SUM(order_item.price*order_item.quantity) as summa 
        from order_s 
        INNER JOIN contract ON contract.id = order_s.fk_contract_id  
        INNER JOIN events ON events.event_driven_id = order_s.id 
        INNER JOIN order_state_event ON order_state_event.event_id = events.id 
        INNER JOIN order_item ON order_item.fk_order_id = order_s.id 
        INNER JOIN (select os.id, max(e.created_when) as max_date from order_s os inner join events e on e.event_driven_id = os.id where e.event_type = 'ORDER_STATE' group by os.id) order_last_date on order_last_date.id = order_s.id 
        where events.created_when = order_last_date.max_date and order_state_event.order_state = 'ORDER_RESULTS' 
        AND order_s.created_when::date >= '{}' AND order_s.created_when::date <= '{}'
        GROUP BY contract.fk_customer_id)
    ORDER_RESULTS on ORDER_RESULTS.customer = c.fk_customer_id
left join
    (select count(distinct order_s.id) as count, contract.fk_customer_id as customer, SUM(order_item.price*order_item.quantity) as summa 
        from order_s 
        INNER JOIN contract ON contract.id = order_s.fk_contract_id  
        INNER JOIN events ON events.event_driven_id = order_s.id 
        INNER JOIN order_state_event ON order_state_event.event_id = events.id 
        INNER JOIN order_item ON order_item.fk_order_id = order_s.id 
        INNER JOIN (select os.id, max(e.created_when) as max_date from order_s os inner join events e on e.event_driven_id = os.id where e.event_type = 'ORDER_STATE' group by os.id) order_last_date on order_last_date.id = order_s.id 
        where events.created_when = order_last_date.max_date and order_state_event.order_state = 'AGREED_BY_SUPPLIER' 
        AND order_s.created_when::date >= '{}' AND order_s.created_when::date <= '{}'
        GROUP BY contract.fk_customer_id)
    AGREED_BY_SUPPLIER on AGREED_BY_SUPPLIER.customer = c.fk_customer_id
where c.fk_customer_id NOT IN (1232844, 1788755, 1784971, 1784576, 1787831, 1793943, 1788809, 1792883, 1787871, 1787947, 8405381) 
AND os.created_when::date >= '{}' AND os.created_when::date <= '{}'
group by c.fk_customer_id, a.name, closed.count, cancel.count, pf.summa, closed.summa, cancel.summa, pf.sups, pf.vsego, payment.count, payment.summa, execution.count, execution.summa, new.count, new.summa, 
PAYMENT_RECEIVED.count, PAYMENT_RECEIVED.summa, RECEPTION.count, RECEPTION.summa, ORDER_RESULTS.count, ORDER_RESULTS.summa, AGREED_BY_SUPPLIER.count, AGREED_BY_SUPPLIER.summa
order by count(os.id)""".format(first_day_last_month, lastMonth.isoformat(), first_day_last_month, lastMonth.isoformat(), first_day_last_month, lastMonth.isoformat(), first_day_last_month, lastMonth.isoformat(), first_day_last_month, lastMonth.isoformat(), first_day_last_month, lastMonth.isoformat(), first_day_last_month, lastMonth.isoformat(), first_day_last_month, lastMonth.isoformat(), first_day_last_month, lastMonth.isoformat(), first_day_last_month, lastMonth.isoformat(), first_day_last_month, lastMonth.isoformat())

df2 = pd.read_sql_query(query,db_connection)
df2.to_excel(writer, encoding='utf8', sheet_name="Отчет№12", index=False, freeze_panes = (1,1))
worksheet = writer.sheets["Отчет№12"]

for idx, col in enumerate(df2):
    if df2[col].count() > 2 and df2[col][1] is not None and df2[col].dtypes != 'int64' and df2[col].dtypes != 'float64':
        series = df2[col][1]
    else:
        series = df2[col].name
    max_len = len(str(series))+1
    worksheet.set_column(idx, idx, max_len)

df2 = df2.fillna(0)

#df2.to_excel(writer, encoding='utf8', sheet_name="Отчет№5", index=False, freeze_panes = (1,1))

df2.to_excel(writer, encoding='utf8', sheet_name="Отчет№12", index=False, freeze_panes = (1,1))

writer.save()
send_mail(send_from="report@emsoft.ru", send_to=["report@emsoft.ru", "kaloria@mail.ru"], subject="Отчет №12", text=" Еженедельный накопительный отчет по всем статусам заказов всех НУЗов с {} по {} \n\nС уважением,\nООО \"Эмсофт\"\n+7 (495) 230-23-48\nreport@emsoft.ru\n".format(first_day_last_month, lastMonth.isoformat()), files=file)


