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
file = "/home/strateg-ai/order_scripts/xlsx_files/Детализация_отчет_по_незакрытым_заказам_от_{}.xlsx".format(today.isoformat())

#nyz = {1784833: ["mik-nuz@mail.ru", "report@emsoft.ru", "lonshakovakristin@gmail.com"], 1784740: ["nuz-secretar@medrzd29.ru", "report@emsoft.ru", "lonshakovakristin@gmail.com"]}

db_connection = create_engine('postgresql://reports:1@localhost:5432/strateg_2') #1@localhost
session_2 = Session(db_connection)

for nyz_i in nyz:
    writer = pd.ExcelWriter(file, engine='xlsxwriter', options={'remove_timezone': True})

    query = """select order_s.order_number as "Номер заказа",  contract.contract_number as "Номер договора", order_s.created_when::date as "Дата создания заказа", agent.name as "Имя заказчика", employee.surname as "Фамилия пользователя", employee.first_name as "Имя пользователя", employee.mail as "Почта", 
temp.name as "Имя поставщика", temp.inn as "ИНН поставщика", SUM(order_item.price*order_item.quantity) as "Сумма заказа", order_state_event.order_state as "Статус заказа", events.created_when as "Дата изменения статуса" 
from order_s 
INNER JOIN contract ON contract.id = order_s.fk_contract_id 
INNER JOIN customer ON customer.id = contract.fk_customer_id 
inner join agent on agent.fk_customer_id = customer.id 
INNER JOIN supplier ON supplier.id = contract.fk_supplier_id 
INNER JOIN (SELECT name, inn, fk_supplier_id from agent) temp ON temp.fk_supplier_id = supplier.id 
INNER JOIN order_item ON order_item.fk_order_id = order_s.id 
INNER JOIN user_s ON user_s.user_name = order_s.created_by 
left JOIN employee ON employee.fk_user_id = user_s.id  
INNER JOIN events ON events.event_driven_id = order_s.id 
INNER JOIN order_state_event ON order_state_event.event_id = events.id
where customer.id = {} 
and (events.event_driven_id, events.created_when) IN (SELECT events.event_driven_id, MAX(events.created_when) FROM events WHERE event_type = 'ORDER_STATE' GROUP BY events.event_driven_id)
and order_state_event.order_state != 'ORDER_CANCELED' and order_state_event.order_state != 'ORDER_CLOSED' and order_state_event.order_state != 'ORDER_CLOSED_POSTFACTUM'
GROUP BY agent.name, order_s.order_number, order_s.created_when, temp.name, temp.inn, employee.surname, employee.first_name, employee.mail, order_state_event.order_state, events.created_when, contract.contract_number
ORDER BY agent.name, order_s.order_number""".format(nyz_i)

    df2 = pd.read_sql_query(query,db_connection)
    df2.to_excel(writer, encoding='utf8', sheet_name="Отчет", index=False, freeze_panes = (1,1))
    worksheet = writer.sheets["Отчет"]
    df = pd.read_sql_query("""select fk_customer_id as "Идентификатор", name as "Имя" from agent where fk_customer_id = {}""".format(nyz_i), db_connection)
    for idx, col in enumerate(df2):
        if df2[col].count() > 2 and df2[col][1] is not None and df2[col].dtypes != 'int64' and df2[col].dtypes != 'float64':
            series = df2[col][1]
        else:
            series = df2[col].name
        max_len = len(str(series))+1
        worksheet.set_column(idx, idx, max_len)

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

    df2.to_excel(writer, encoding='utf8', sheet_name="Отчет", index=False, freeze_panes = (1,1))

    writer.save()
    send_mail(send_from="report@emsoft.ru", send_to=nyz[nyz_i], subject="Отчет для {}".format(df["Имя"].values[0]), text="""Детализация отчета по незакрытым заказам от {}. 
В случае если вложенный файл приходит с расширением *.dat, то необходимо переименовать его в *.xlsx.
Также данная неполадка решается путем настройки почтового клиента.

С уважением,
ООО "Эмсофт"
+7 (495) 230-23-48
report@emsoft.ru""".format(today.isoformat()), files=file)

