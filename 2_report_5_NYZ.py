# email_imports
from modules.sendmail_excel import send_mail
from modules.email_address import nyz

# SQL imports
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
import pandas as pd
import seaborn as sns
import numpy as np

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
file = "/home/strateg-ai/order_scripts/xlsx_files/Ежемесячный_отчет_с_заказами_содержащими_товары_с_НДС_0%_с_{}_по_{}.xlsx".format(first_day_last_month, lastMonth.isoformat())

#nyz = {1784579: ["MSlepcova@klgd.ru", "report@emsoft.ru"], 1784593: ["OGajvoronskaya@svrw.ru", "report@emsoft.ru"], 1784602: ["nuz-VikulovaEA@wsr.ru", "report@emsoft.ru"], 1784662: ["nuz3-chunarevvf@wsr.ru", "report@emsoft.ru"], 1784695: ["hospital-zlt-hospital@dzo.surw.ru", "report@emsoft.ru"], 1784704: ["hospital-kurgan-secretar@dzo.surw.ru", "report@emsoft.ru"], 1784749: ["okb-brnl-EGEgorova@wsr.ru", "report@emsoft.ru"], 1784767: ["nuz-IVEvstegneeva@skzd.ru", "report@emsoft.ru", "kokb-sekret@mail.ru"], 1784782: ["hospital-orenburg-mash@dzo.surw.ru", "report@emsoft.ru"], 1784779: ["nuz_MarhotinaVA@orw.ru", "report@emsoft.ru"], 1784800: ["MO12Secretar@orw.ru", "report@emsoft.ru"], 1784851: ["uzbruz-secret@kbsh.ru", "report@emsoft.ru"], 1784824: ["rdmo-KuskovEY@wsr.ru", "report@emsoft.ru"], 1784830: ["nuz-kartaly-sekretar@dzo.surw.ru", "report@emsoft.ru"], 1784863: ["guz-BrashenkoEV@wsr.ru", "report@emsoft.ru"], 1784878: ["hospital-buzuluk-secretar@dzo.surw.ru", "report@emsoft.ru"], 1784944: ["BOLKEM_Manoylov@orw.ru", "report@emsoft.ru"], 1784953: ["nuz3-NalbandyanAG@wsr.ru", "report@emsoft.ru"], 1784968: ["Luporova@svrw.ru", "report@emsoft.ru"], 1784983: ["EVErmakovaTm@skzd.ru", "report@emsoft.ru", "tskzd1@yandex.ru"], 1785046: ["nvsb14-UmerkinMSh@kbsh.ru", "report@emsoft.ru"], 1785049: ["nuz-KomissarovaOA@nrr.ru", "report@emsoft.ru", "nuzupbuy@gmail.ru"], 1785022: ["rdmo-LavrentyevaKE@wsr.ru", "report@emsoft.ru"], 1785013: ["Bolnicatomsk2@wsr.ru", "report@emsoft.ru"], 1784929: ["nuz-BakaAI@nrr.ru", "report@emsoft.ru", "baka.alexnder@yandex.ru"], 1785010: ["DKB-Sekretar@kbsh.ru", "report@emsoft.ru"], 1784857: ["nuz_vyborg@orw.ru", "report@emsoft.ru"], 1784866: ["Thohlova@svrw.ru", "report@emsoft.ru"], 1784872: ["Suhospscr@svrw.ru", "report@emsoft.ru"], 1784890: ["nuz-rub-BobkovaNV@wsr.ru", "report@emsoft.ru"], 1784722: ["s2k-kuuzb_secr@svrw.ru", "report@emsoft.ru"], 1784641: ["nuz-PlishevskayaNY@wsr.ru", "report@emsoft.ru"], 1784599: ["SNOdintsov@svrw.ru", "report@emsoft.ru"], 1784731: ["lpu_IvanovaEA@dvgd.rzd.", "report@emsoft.ru", "nuz.sek@gmail.com"], 1784776: ["ENVolik@skzd.ru", "report@emsoft.ru", "glav_vrach@nuzokbmv.ru"]}

db_connection = create_engine('postgresql://reports:1@localhost:5432/strateg_2') #1@localhost
session_2 = Session(db_connection)

#writer = pd.ExcelWriter(file, engine='xlsxwriter', options={'remove_timezone': True})

for nyz_i in nyz:
    writer = pd.ExcelWriter(file, engine='xlsxwriter', options={'remove_timezone': True})
    query = """select order_s.order_number as "Номер заказа", contract.contract_number as "Номер договора", order_s.created_when::date as "Дата создания заказа", agent.name as "Имя заказчика", employee.surname as "Фамилия пользователя", 
employee.first_name as "Имя пользователя", employee.mail as "Почта", temp.name as "Имя поставщика", temp.inn as "ИНН поставщика", order_item.price as "Цена за ед.товара", order_item.quantity as "Кол-во товара в заказе", 
SUM(order_item.price*order_item.quantity) as "Сумма позиции", suppliers_product.tax_percent as "НДС", order_item.product_name as "Наименование товара", product_group.name as "Категория", order_state_event.order_state as "Статус заказа" 
from order_s 
INNER JOIN contract ON contract.id = order_s.fk_contract_id 
INNER JOIN customer ON customer.id = contract.fk_customer_id 
inner join agent on agent.fk_customer_id = customer.id 
INNER JOIN supplier ON supplier.id = contract.fk_supplier_id 
INNER JOIN (SELECT name, inn, fk_supplier_id from agent) temp ON temp.fk_supplier_id = supplier.id 
RIGHT JOIN order_item ON order_item.fk_order_id = order_s.id 
INNER JOIN user_s ON user_s.user_name = order_s.created_by 
INNER JOIN employee ON employee.fk_user_id = user_s.id 
left join suppliers_product on suppliers_product.id = order_item.fk_supplier_product_id 
left join product_name on product_name.id = suppliers_product.fk_product_name_id
left join product on product.id = product_name.fk_product_id 
left join product_group on product_group.id = product.fk_product_group_id 
INNER JOIN events ON events.event_driven_id = order_s.id 
INNER JOIN order_state_event ON order_state_event.event_id = events.id 
where customer.id = {} 
and (events.event_driven_id, events.created_when) IN (SELECT events.event_driven_id, MAX(events.created_when) FROM events WHERE event_type = 'ORDER_STATE' GROUP BY events.event_driven_id) 
and suppliers_product.tax_percent = 0 
 AND order_s.created_when::date >= '{}' AND order_s.created_when::date <= '{}'
GROUP BY agent.name, order_s.order_number, order_s.created_when, temp.name, temp.inn, employee.surname, employee.first_name, employee.mail, order_item.product_name, product_group.name, order_item.price, order_item.quantity, contract.contract_number, contract.id, order_state_event.order_state, suppliers_product.tax_percent 
ORDER BY order_s.order_number, agent.name""".format(nyz_i, first_day_last_month, lastMonth.isoformat())

    df2 = pd.read_sql_query(query,db_connection)
    df2.to_excel(writer, encoding='utf8', sheet_name="Отчет", index=False, freeze_panes = (1,1))
    worksheet = writer.sheets["Отчет"]
    df = pd.read_sql_query("""select fk_customer_id as "Идентификатор", name as "Имя" from agent where fk_customer_id = {}""".format(nyz_i), db_connection)
    #print(df["Имя"].values[0])
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

    for idx, row in enumerate(df2["НДС"]):
        if row == -1:
            df2.loc[idx, "НДС"] = 'Без НДС'

    for idx, col in enumerate(df2):
        if df2[col].dtype in ('object', 'string_', 'unicode_') and df2[col].name not in ('Дата создания заказа', 'Категория'):
            max_len = df2[col].map(len).max()+1
        else:
            series = df2[col].name
            max_len = len(str(series))+1
        worksheet.set_column(idx, idx, max_len)
    
    df2.to_excel(writer, encoding='utf8', sheet_name="Отчет", index=False, freeze_panes = (1,1))
    writer.save()
    send_mail(send_from="report@emsoft.ru", send_to=nyz[nyz_i], subject="Отчет для {}".format(df["Имя"].values[0]), text="""Ежемесячный отчет с заказами, содержащими товары с НДС 0% c {} по {}.
В случае если вложенный файл приходит с расширением *.dat, то необходимо переименовать его в *.xlsx.
Также данная неполадка решается путем настройки почтового клиента.

С уважением,
ООО "Эмсофт"
+7 (495) 230-23-48
report@emsoft.ru""".format(first_day_last_month, lastMonth.isoformat()), files=file)

