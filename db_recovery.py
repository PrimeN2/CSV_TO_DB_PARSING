import pandas as pd
import sqlite3

conn = sqlite3.connect('db/recovered.db')

FOLDER = "csv/"

config = {
    'Clients.csv': {
        'rename': {'Client_ID': 'client_id', 'Покупатель': 'client_name'},
        'dtype': {'client_id': 'TEXT', 'client_name': 'TEXT'}
    },
    'Products.csv': {
        'rename': {
            'Product_Key': 'product_key',
            'Название товара': 'product_name',
            'Название бренда': 'brand_name',
            'Группа товара': 'product_group'
        },
        'dtype': {
            'product_key': 'TEXT',
            'product_name': 'TEXT',
            'brand_name': 'TEXT',
            'product_group': 'TEXT'
        }
    },
    'Sales.csv': {
        'rename': {
            'Себестоимость': 'cost',
            'Дата покупки': 'purchase_date',
            'Номер чека': 'check_number',
            'Product_key': 'product_key',
            'Филиал': 'branch',
            'Количество': 'quantity',
            'Сумма покупки': 'purchase_amount',
            'Сумма скидки': 'discount_amount',
            'Client_ID': 'client_id',
            'Машина доставки': 'delivery_car',
            'Адрес': 'address'
        },
        'dtype': {
            'cost': 'REAL',
            'purchase_date': 'DATE',
            'check_number': 'TEXT',
            'product_key': 'TEXT',
            'branch': 'TEXT',
            'quantity': 'INTEGER',
            'purchase_amount': 'REAL',
            'discount_amount': 'REAL',
            'client_id': 'TEXT',
            'delivery_car': 'TEXT',
            'address': 'TEXT'
        },
        'parse_dates': ['Дата покупки']
    },
    'Sums_Check.csv': {
        'rename': {
            'Дата покупки': 'purchase_date',
            'Client_ID': 'client_id',
            'Адрес': 'address',
            'Машина доставки': 'delivery_car',
            'Филиал': 'branch',
            'Номер чека': 'check_number',
            'Сумма покупки|Сумма': 'purchase_sum'
        },
        'dtype': {
            'purchase_date': 'DATE',
            'client_id': 'TEXT',
            'address': 'TEXT',
            'delivery_car': 'TEXT',
            'branch': 'TEXT',
            'check_number': 'TEXT',
            'purchase_sum': 'REAL'
        },
        'parse_dates': ['Дата покупки']
    }
}

for file, cfg in config.items():
    df = pd.read_csv(
        FOLDER + file,
        sep='\t',
        encoding='utf-8',
        parse_dates=cfg.get('parse_dates', []),
        dayfirst=True
    )

    print(file)
    
    if file == 'Sales.csv':
        numeric_cols = ['Себестоимость', 'Сумма скидки', 'Сумма покупки']
        for col in numeric_cols:
            df[col] = df[col].astype(str).str.replace(',', '.')
        
        df['Себестоимость'] = pd.to_numeric(df['Себестоимость'], errors='coerce')
        df['Сумма скидки'] = pd.to_numeric(df['Сумма скидки'], errors='coerce')
        
        df['Сумма скидки'] = df['Сумма скидки'].round(4)

    if file == 'Sums_Check.csv':
        df['Сумма покупки|Сумма'] = df['Сумма покупки|Сумма'].astype(str).str.replace(',', '.')
            
        df['Сумма покупки|Сумма'] = pd.to_numeric(df['Сумма покупки|Сумма'], errors='coerce')
    
    df.rename(columns=cfg['rename'], inplace=True)
    
    df.to_sql(
        name=file.split('.')[0].lower(),
        con=conn,
        if_exists='replace',
        index=False,
        dtype=cfg['dtype']
    )

conn.close()