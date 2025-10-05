import pandas as pd
import requests
import pyodbc
from datetime import datetime, timedelta
from io import StringIO
import time

API_TOKEN = 'уникальный токен'
URL = 'уникальная ссылка.csv'


def getdate():
    return (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
# def getdata_api(start_date, end_date):
#     params = {
#                 'date1': start_date,
#                 'date2': end_date,
def getdata_api(date):
    params = {
                'date1': date,
                'date2': date,
                'id': 27730488,
                'metrics': 'ym:s:visits,ym:s:users,ym:s:bounceRate',
                'dimensions': """ym:s:<attribution>UTMSource,ym:s:date,ym:s:startURL""",
                'attribution': "cross_device_last_significant",
                'limit': 100000
             }

    header_params = {
                        'GET': '/management/v1/counters HTTP/1.1',
                        'Host': 'api-metrika.yandex.ru',
                        'Authorization': 'OAuth ' + API_TOKEN,
                        'Content-Type': 'application/x-yametrika+json'
                    }

    response = requests.get(
                                URL,
                                params=params,
                                headers=header_params
                            )

    return pd.read_csv(StringIO(response.text), encoding='utf-8')

def etl_data(df):
    df = df[(df['UTM Source'] != 'Итого и средние')]  # Фильтрация данных
    df.loc[:, 'Дата визита'] = pd.to_datetime(df['Дата визита'], format='%Y-%m-%d')  # Преобразуем в datetime с помощью .loc[]
    return df.sort_values(by=['Дата визита', 'Визиты'], ascending=[True, False]).reset_index(drop=True)

def insert_db(df):
    cnxn = pyodbc.connect('DRIVER={SQL Server Native Client 10.0};'
                          'SERVER=s-fin.dc.centrzaimov.ru,4644;'
                          'DATABASE=Pesochnica;'
                          'Trusted_connection=yes;')
    cursor = cnxn.cursor()
    for index, row in df.iterrows():
        # Преобразуем wmid_value в строку, если оно не NaN, или в None
        wmid_value = row['wmid_value'] if pd.notna(row['wmid_value']) else "пусто"

        # Преобразуем Визиты и Посетители в float, если они могут быть интерпретированы как числа
        try:
            visits = float(row['Визиты']) if pd.notna(row['Визиты']) else 0.0
        except ValueError:
            visits = 0.0  # В случае ошибки присваиваем 0

        try:
            users = float(row['Посетители']) if pd.notna(row['Посетители']) else 0.0
        except ValueError:
            users = 0.0  # В случае ошибки присваиваем 0

        # Отказы (bounceRate — доля от 0 до 1)
        try:
            bounceRate = float(row['Отказы']) if pd.notna(row['Отказы']) else 0.0
        except (ValueError, TypeError):
            bounceRate = 0.0

        # Запрос без столбца "Страница входа"
        insert_query = """
        INSERT INTO yandex_metrika ([Дата визита], [UTM Source], [Визиты], [Посетители], [wmid_value], [Отказы])
        VALUES (?, ?, ?, ?, ?, ?)
        """
        cursor.execute(insert_query, row['Дата визита'], row['UTM Source'], visits, users, wmid_value, bounceRate)

    cnxn.commit()
    cursor.close()
    cnxn.close()

def main():
    # start_date = '2025-09-26'  # Начальная дата
    # end_date = '2025-09-29'  # Конечная дата
    # df = getdata_api(start_date, end_date)  # Передаем диапазон дат
    date = getdate()
    df = getdata_api(date)
    df = etl_data(df)
    # Извлекаем значение после "wmid=" (включая возможность фигурных скобок)
    df['wmid_value'] = df['Страница входа'].str.extract(r'wmid=({[\w\d]+})|wmid=([\w\d]+)')[0].fillna(
        df['Страница входа'].str.extract(r'wmid=({[\w\d]+})|wmid=([\w\d]+)')[1])
    df['wmid_value'] = df['wmid_value'].fillna('')
    # df = df.groupby(['Дата визита', 'UTM Source', 'wmid_value'])[['Визиты', 'Посетители', 'Отказы']].sum()
    df = df.groupby(['Дата визита', 'UTM Source', 'wmid_value']).agg({
        'Визиты': 'sum',
        'Посетители': 'sum',
        'Отказы': 'mean'  # или 'median', но обычно 'mean'
    })
    # Убираем мультииндекс
    df = df.reset_index()
    # Заменяем пустые значения в 'wmid_value' на пустую строку (или на None)
    df['wmid_value'] = df['wmid_value'].fillna('')  # Можно использовать '' или None
    df = df[['Дата визита', 'UTM Source', 'Визиты', 'Посетители', 'wmid_value', 'Отказы']]
    insert_db(df)
    print("Данные сохранены")
    time.sleep(10)



if __name__ == "__main__":
    main()