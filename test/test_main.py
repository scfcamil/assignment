from etl_main.main import *
from datetime import datetime
from pytest_mock_resources import create_mongo_fixture
from pymongo import MongoClient

db = read_csv('./test/test-employee-details.csv')

def test_read_csv():
    assert (db.columns == ['EmployeeID','FirstName','  LastName  ','BirthDate','Department','Salary']).all()
    assert list(db.iloc[0,:].values) == ['E001    ','Alice%  ','   White    ','1990-06-12',' Finance  ', 55000]

db_a = read_csv('./test/test-employee-details-expected-a.csv')

def test_headers_space_removal():
    assert (headers_space_removal(db).values == db_a.values).all()

db_b = read_csv('./test/test-employee-details-expected-b.csv')

def test_char_only():
    assert (char_only(db_a, ['FirstName','LastName']).values == db_b.values).all()

db_c = read_csv('./test/test-employee-details-expected-c.csv')

def test_strip_columns():
    assert (strip_columns(db_b, ['FirstName','LastName']).values == db_c.values).all()

db_d = read_csv('./test/test-employee-details-expected-d.csv')

def test_add_full_name():
    assert (add_full_name(db_c, 'FirstName', 'LastName', 'FullName').values == db_d.values).all()

db_e = read_csv('./test/test-employee-details-expected-e.csv')
date_format = '%d/%m/%Y'

def test_change_date_format():
    assert (change_date_format(db_d['BirthDate'],date_format).values == db_e['BirthDate'].values).all()

db_f = read_csv('./test/test-employee-details-expected-f.csv')

reference_date = datetime.strptime('01/01/2023', date_format)

def test_add_age():
    assert (add_age(db_e, 'BirthDate', reference_date, date_format, 'Age').values == db_f.values).all()


def test_salary_class():
    assert salary_class('abcde') == 'NA'
    assert salary_class('50') == 'B'
    assert salary_class(49.3) == 'A'

db_g = read_csv('./test/test-employee-details-expected-g.csv')

def test_transform_data():
    assert (transform_data(db).values == db_g.values).all()


