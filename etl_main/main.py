from pandas import read_csv as rcsv, to_datetime, isna
from pymongo import MongoClient
from logging import basicConfig, INFO, getLogger
from datetime import datetime

def setup_logging():
    basicConfig(level = INFO)
    the_logger = getLogger()
    return the_logger

logger = setup_logging()

def read_csv(input_file):
    logger.info(f"reading {input_file}")
    db = rcsv(input_file, header=0)
    return db

def char_only(db, columns:list):
    for column in columns:
        db[column] = db[column].str.replace('[^a-zA-Z.\- ]', '')
    return db

def strip_columns(db, columns:list):
    for column in columns:
        db[column] = db[column].str.strip()
    return db

def salary_class(salary):
    try:
        salary = float(salary)
    except:
        return 'NA'
    if salary < 50:
        return 'A'
    if 50 <= salary <= 100:
        return 'B'
    if salary > 100:
        return 'C'

def headers_space_removal(db):
    logger.info('removing leading and trailing spaces in headers')
    db.rename(columns=lambda x: x.strip(), inplace=True)
    return db

def change_date_format(pd_series, date_format):
    logger.info('changing date format')
    return to_datetime(pd_series, errors='coerce').dt.strftime(date_format)

def add_full_name(db, name_col:str, surname_col:str, fullname_col:str):
    logger.info(f'adding {fullname_col}')
    db[fullname_col] = db[name_col] + ' ' + db[surname_col]
    return db

def add_age(db, dob:str, ref, date_format, age:str):
    db[age] = db[dob].apply(lambda i: 'NaN' if isna(i) else (ref-datetime.strptime(i, date_format)).days//365)
    return db

def transform_data(db):
    db = headers_space_removal(db)
    # logger.info('dropping invalid row')
    # db.drop(10, axis=0, inplace=True)
    db = char_only(db, ['FirstName','LastName'])
    db = strip_columns(db, ['FirstName','LastName','EmployeeID','Department'])
    db = add_full_name(db,'FirstName','LastName','FullName')
    date_format = '%d/%m/%Y'
    db['BirthDate'] = change_date_format(db['BirthDate'], date_format)
    reference_date = datetime.strptime('01/01/2023', date_format)
    db = add_age(db, 'BirthDate', reference_date, date_format, 'Age')
    db['SalaryBucket'] = db['Salary'].apply(lambda i: salary_class(i))
    db.drop(['FirstName','LastName','BirthDate'], axis=1, inplace=True)
    return db


def load_data(pandas_df, client):
    try:
        logger.info('creating mongodb database')
        mongo_database = client['businessdb']
        logger.info('creating mongodb collection')
        collection = mongo_database['employees']
        logger.info('creating documents')
    except:
        logger.error('mongo error')
    try:
        documents = pandas_df.to_dict(orient = 'records')
        logger.info('inserting documents into collection')
        collection.insert_many(documents)
        logger.info('testing finding documents')
        mydoc = collection.find_one()
        for x in mydoc:
            logger.info(x)
    except Exception as ex:
        logger.error(ex)

def get_mongo_client():
    try:
        logger.info('creating mongodb client')
        client = MongoClient('mongodb://root:example@mongo:27017/')
    except Exception as ex:
        logger.error(f'unable to create mongo client due to {ex}')
    return client

def main(input_file, mongo):
    db = read_csv(input_file)
    db = transform_data(db)
    load_data(db, mongo)

if __name__ == "__main__":
    mongo_db_client = get_mongo_client()
    main('employee-details.csv', mongo_db_client)
