import pyodbc
import csv
from datetime import datetime
import os

# Параметры подключения
server = '192.168.17.11'
database = 'MEDIALOG'  # Замените на имя вашей БД
username = 'sa'
password = '*69'

connection_string = f'DRIVER={{SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'

try:
    conn = pyodbc.connect(connection_string)
    print("Успешное подключение к SQL Server 2005!")

    cursor = conn.cursor()

    query = """
    SELECT 
        FM_BILL.FM_BILL_ID,
        CONVERT(VARCHAR(10), FM_BILL.BILL_DATE, 120) AS BILL_DATE,
        FM_SERV.CODE AS SERVICE_CODE,
        FM_SERV.CODE_AN2 AS SERVICE_NAME,
        FM_BILLDET.CNT AS SERVICE_COUNT,
        FM_BILLDET.PRICE_TO_PAY AS PRICE,
        FM_BILLDET.ORG_FIXED_AMOUNT AS DISCOUNT
    FROM 
        FM_BILLDET
    JOIN 
        FM_BILL ON FM_BILL.FM_BILL_ID = FM_BILLDET.FM_BILL_ID
    LEFT JOIN 
        FM_SERV ON FM_SERV.FM_SERV_ID = FM_BILLDET.FM_SERV_ID
    WHERE 
        FM_BILL.FM_ORG_ID = 1
        AND FM_BILL.BILL_DATE = '2025-12-29'
        AND FM_SERV.CODE LIKE '18_____'
        AND FM_BILLDET.CANCEL =0
    """

    cursor.execute(query)
    rows = cursor.fetchall()

    # Подсчет итогов
    total_count = 0
    total_price = 0.0

    # Создание имени файла с временной меткой
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Определение пути для сохранения файла
    save_dir = r'D:\Pavel\medi_net\temp\mri'

    # Создание папки, если она не существует
    os.makedirs(save_dir, exist_ok=True)

    # Полный путь к файлу
    filename = f"report_{timestamp}.csv"
    full_path = os.path.join(save_dir, filename)

    # Сохранение в CSV файл
    with open(full_path, 'w', newline='', encoding='utf-8-sig') as csvfile:
        fieldnames = ['BILL_ID', 'DATE', 'SERVICE_CODE', 'SERVICE_PRICE']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames, delimiter=';')

        # Запись заголовка
        writer.writeheader()

        print("\nРезультаты запроса:")
        print("{:<10} {:<12} {:<15} {:<30} {:<10} {:<10} {:<10}".format(
            "BILL_ID", "DATE", "SERVICE_CODE", "SERVICE_NAME", "COUNT", "PRICE", "DISCOUNT"))
        print("-" * 100)

        for row in rows:
            # Обработка NULL-значений
            bill_id = row.FM_BILL_ID if row.FM_BILL_ID is not None else ''
            date = row.BILL_DATE if row.BILL_DATE is not None else ''
            service_code = row.SERVICE_CODE if row.SERVICE_CODE is not None else ''
            service_name = row.SERVICE_NAME if row.SERVICE_NAME is not None else ''
            count = row.SERVICE_COUNT if row.SERVICE_COUNT is not None else 0
            price = float(row.PRICE) if row.PRICE is not None else 0.0
            discount = float(row.DISCOUNT) if row.DISCOUNT is not None else 0.0

            # Суммируем для итогов
            total_count += count
            total_price += price

            print("{:<10} {:<12} {:<15} {:<30} {:<10} {:<10.2f} {:<10.2f}".format(
                bill_id, date, service_code, service_name, count, price, discount))

            # Запись строки в CSV
            writer.writerow({
                'BILL_ID': bill_id,
                'DATE': date,
                'SERVICE_CODE': service_code,
                'SERVICE_PRICE': price,
            })

        # # Запись итоговой строки в CSV
        # writer.writerow({
        #     'BILL_ID': '',
        #     'DATE': '',
        #     'SERVICE_CODE': '',
        #     'SERVICE_NAME': 'ИТОГО:',
        #     'COUNT': total_count,
        #     'PRICE': total_price,
        #     'DISCOUNT': ''
        # })

    # Вывод итоговой строки в консоль
    print("-" * 100)
    print("{:<10} {:<12} {:<15} {:<30} {:<10} {:<10.2f} {:<10}".format(
        "", "", "", "ИТОГО:", total_count, total_price, ""))

    print(f"\nДанные сохранены в файл: {full_path}")

    cursor.close()

except pyodbc.Error as e:
    print("Ошибка при выполнении запроса:", e)

finally:
    if 'conn' in locals():
        conn.close()
        print("\nСоединение с SQL Server закрыто.")