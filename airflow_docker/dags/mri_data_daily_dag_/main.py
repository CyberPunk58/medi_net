from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pyodbc
import csv
import os


# =====================================================
# Функция, которая выполняет вашу логику
# =====================================================
def generate_mri_report():
    # Параметры подключения
    server = '192.168.17.11'
    database = 'MEDIALOG'
    username = 'sa'
    password = '*69'

    connection_string = f'DRIVER={{SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'

    try:
        conn = pyodbc.connect(connection_string)
        print("✅ Успешное подключение к SQL Server!")

        cursor = conn.cursor()

        # Вчерашняя дата
        yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

        query = f"""
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
            AND FM_BILL.BILL_DATE = '{yesterday}'
            AND FM_SERV.CODE LIKE '18_____'
            AND FM_BILLDET.CANCEL = 0
        """

        print(f"📅 Выполняется запрос за дату: {yesterday}")

        cursor.execute(query)
        rows = cursor.fetchall()

        # Подсчет итогов
        total_count = 0
        total_price = 0.0

        # Путь для сохранения
        save_dir = r'D:\Pavel\medi_net\temp\mri'
        os.makedirs(save_dir, exist_ok=True)

        filename = "report.csv"
        full_path = os.path.join(save_dir, filename)

        # Сохранение в CSV
        with open(full_path, 'w', newline='', encoding='utf-8-sig') as csvfile:
            fieldnames = ['BILL_ID', 'MRI_DATE', 'SERVICE_CODE', 'SERVICE_PRICE']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames, delimiter=';')
            writer.writeheader()

            for row in rows:
                bill_id = row.FM_BILL_ID if row.FM_BILL_ID is not None else ''
                date = row.BILL_DATE if row.BILL_DATE is not None else ''
                service_code = row.SERVICE_CODE if row.SERVICE_CODE is not None else ''
                count = row.SERVICE_COUNT if row.SERVICE_COUNT is not None else 0
                price = float(row.PRICE) if row.PRICE is not None else 0.0

                total_count += count
                total_price += price

                writer.writerow({
                    'BILL_ID': bill_id,
                    'MRI_DATE': date,
                    'SERVICE_CODE': service_code,
                    'SERVICE_PRICE': price,
                })

        print(f"✅ Данные сохранены: {full_path}")
        print(f"📊 ИТОГО: записей={total_count}, сумма={total_price:.2f}")

        cursor.close()

    except pyodbc.Error as e:
        print(f"❌ Ошибка SQL: {e}")
        raise  # Важно: пробросить ошибку, чтобы AirFlow знал о неудаче

    finally:
        if 'conn' in locals():
            conn.close()
            print("🔒 Соединение закрыто.")


# =====================================================
# Определение DAG
# =====================================================
default_args = {
    'owner': 'pavel',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 2,  # Попыток при ошибке
    'retry_delay': timedelta(minutes=5),  # Задержка между попытками
}

with DAG(
        dag_id='mri_daily_report',  # Имя DAG (уникальное)
        default_args=default_args,
        schedule_interval='45 16 * * *',  # Каждый день в 6:00 утра
        catchup=False,  # Не выполнять пропущенные запуски
        tags=['mri', 'report', 'daily'],  # Теги для удобной фильтрации
) as dag:
    # Задача генерации отчёта
    generate_report = PythonOperator(
        task_id='generate_mri_report',
        python_callable=generate_mri_report,
        
    )