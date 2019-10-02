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
file = "/home/strateg-ai/order_scripts/xlsx_files/Отчет_№8_Ежемесячный_отчет_по_всем_заказам_отсортированный_по_дирекциям_с_{}_по_{}.xlsx".format(first_day_last_month, lastMonth.isoformat())

directions = [[[1784605,1784752,1784698,1784731,1784794,1785040,1784809,1785001,1784869], "Дальневосточная дирекция здравоохранения"],
[[1784644,1784785,1784818,1785079,1784965,1784629,1784899,1784935], "Красноярская дирекция здравоохранения"],
[[1784626,1784767,1784770,1784776,1784941,1784860,1784827,1784992,1785082,1784983,1785016,1784902,1784959,1785028,1784932,1785025], "Северо-Кавказская дирекция здравоохранения"],
[[1784602,1784749,1784662,1784641,1785004,1784824,1784938,1784863,1784953,1784890,1784977,1785013,1785022], "Западно-Сибирская дирекция здравоохранения"],
[[1784608,1784647,1784695,1784704,1784782,1784947,1784878,1784830,1784845], "Южно-Уральская дирекция здравоохранения"],
[[1784638,1784743,1784800,1784779,1784803,1784773,1784815,1784857,1784887,1784944,1784896,1784908], "Октябрьская дирекция здравоохранения"],
[[1784596,1784701,1784758,1784761,1784656,1784590,1784989,1785019,1785037], "Горьковская дирекция здравоохранения"],
[[1784635,1784671,1784755,1784689,1784710], "Приволжская дирекция здравоохранения"],
[[1785007,1784582,1784788,1784746,1784848,1785031,1785061,1784920,1785073,1784812,1792480,1785067,1784923], "Восточно-Сибирская дирекция здравоохранения"],
[[1784617,1784821,1793057,1784680,1784707,1784725,1784884,1784623], "Юго-Восточная дирекция здравоохранения"],
[[1784614,1785064,1784881,1785043,1784986,1785052,1785076,1784611,1784905,1784911,1784917,1784875,1785058,1784665], "Забайкальская дирекция здравоохранения"],
[[1784593,1784650,1784686,1784866,1784872,1784599,1784620,1784968,1784722,1784659,1790967,1784893], "Свердловская дирекция здравоохранения"],
[[1785010,1784677,1784653,1784764,1784737,1784851,1784974,1784995,1785046], "Куйбышевская дирекция здравоохранения"],
[[1784632,1784683,1784692,1784740,1784734,1784806,1785049,1784833,1784836,1784929,1784854,1784914], "Северная дирекция здравоохранения"],
[[1784998,1784791,1784674,1784668,1784713,1784716,1784719,1784728,1784797,1785055,1784842,1784926,1785034,1785070,1784579], "Московская дирекция здравоохранения"],
[[1784839,949547,1784980,1784962,1784956,1784950], "НУЗ Центрального подчинения"]]

#db_connection = create_engine('postgresql://reports:repOrd2018@195.9.210.19:5432/strateg') #1@localhost
#session = Session(db_connection)
db_connection_2 = create_engine('postgresql://reports:1@localhost:5432/strateg_2') #1@localhost
session_2 = Session(db_connection_2)

writer = pd.ExcelWriter(file, engine='xlsxwriter', options={'remove_timezone': True})
for value in directions:
    sheetname=value[1]
    query_2 = """select order_s.order_number as "Номер заказа", contract.contract_number as "Номер договора", contract.id as "ID договора", order_s.created_when::date as "Дата создания заказа", agent.name as "Имя заказчика", employee.surname as "Фамилия пользователя", employee.first_name as "Имя пользователя", employee.mail as "Почта", temp.name as "Имя поставщика", temp.inn as "ИНН поставщика", order_item.price as "Цена за ед.товара", order_item.quantity as "Кол-во товара в заказе", SUM(order_item.price*order_item.quantity) as "Сумма позиции", suppliers_product.tax_percent as "НДС", order_item.product_name as "Наименование товара", product_group.name as "Категория", order_state_event.order_state as "Статус заказа" from order_s INNER JOIN contract ON contract.id = order_s.fk_contract_id INNER JOIN customer ON customer.id = contract.fk_customer_id inner join agent on agent.fk_customer_id = customer.id INNER JOIN supplier ON supplier.id = contract.fk_supplier_id INNER JOIN (SELECT name, inn, fk_supplier_id from agent) temp ON temp.fk_supplier_id = supplier.id RIGHT JOIN order_item ON order_item.fk_order_id = order_s.id INNER JOIN user_s ON user_s.user_name = order_s.created_by INNER JOIN employee ON employee.fk_user_id = user_s.id left join suppliers_product on suppliers_product.id = order_item.fk_supplier_product_id left join product on product.id = suppliers_product.fk_product_id left join product_group on product_group.id = product.fk_product_group_id INNER JOIN events ON events.event_driven_id = order_s.id INNER JOIN order_state_event ON order_state_event.event_id = events.id INNER JOIN (select os.id, max(e.created_when) as max_date from order_s os inner join events e on e.event_driven_id = os.id where e.event_type = 'ORDER_STATE' group by os.id) order_last_date on order_last_date.id = order_s.id WHERE events.created_when = order_last_date.max_date and customer.id IN ({}) and order_s.created_when::date >= '{}' and order_s.created_when::date <= '{}' GROUP BY agent.name, order_s.order_number, order_s.created_when, temp.name, temp.inn, employee.surname, employee.first_name, employee.mail, order_item.product_name, product_group.name, order_item.price, order_item.quantity, contract.contract_number, contract.id, order_state_event.order_state, suppliers_product.tax_percent ORDER BY order_s.order_number, agent.name""".format(str(value[0]).strip('[]'), first_day_last_month, lastMonth.isoformat())

    #df = pd.read_sql_query(query,db_connection)
    #print(df.dtypes)
    #df["Сумма заказа"] = pd.to_numeric(df["Сумма заказа"])

    df2 = pd.read_sql_query(query_2,db_connection_2)
    df2["Номер заказа"] = pd.to_numeric(df2["Номер заказа"])
    part = sheetname.split(' ')[:2]
    name_sheet = part[0] + ' ' + part[1]
    df2.to_excel(writer, encoding='utf8', sheet_name=name_sheet, index=False, freeze_panes = (1,1))
    worksheet = writer.sheets[name_sheet]

    #Выравнивание длинны столбцов по наименованию столбца
    for idx, col in enumerate(df2):
        if df2[col].dtype in ('object', 'string_', 'unicode_') and df2[col].name not in ('Дата создания заказа', 'Категория'):
            max_len = df2[col].map(len).max()+1
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
    for idx, row in enumerate(df2["НДС"]):
        if row == -1:
            df2.loc[idx, "НДС"] = 'Без НДС'

    df2.to_excel(writer, encoding='utf8', sheet_name=name_sheet, index=False, freeze_panes = (1,1))

writer.save()
send_mail(send_from="report@emsoft.ru", send_to=["report@emsoft.ru", "kaloria@mail.ru"], subject="Отчет №8", text="Ежемесячный отчет по всем заказам отсортированный по дирекциям c {} по {} \n\nС уважением,\nООО \"Эмсофт\"\n+7 (495) 230-23-48\nreport@emsoft.ru\n".format(first_day_last_month, lastMonth.isoformat()), files=file)
