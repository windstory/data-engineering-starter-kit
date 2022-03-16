import psycopg2
import requests

def get_Redshift_connection():
    host = "learnde.cduaw970ssvt.ap-northeast-2.redshift.amazonaws.com"
    redshift_user = "user"  
    redshift_pass = "password"
    port = 5439
    dbname = "dev"
    conn = psycopg2.connect(f"dbname={dbname} user={redshift_user} host={host} password={redshift_pass} port={port}")
    conn.set_session(autocommit=True)
    return conn.cursor()

def extract(url):
    f = requests.get(url)
    return (f.text)

def transform(text):
    lines = text.split("\n")
    return lines

def load(lines):
    cur = get_Redshift_connection()
    sql = "BEGIN;DELETE FROM windstoryya.name_gender;"
    for l in lines:
        if l != '':
            (name, gender) = l.split(",")
            sql += f"INSERT INTO windstoryya.name_gender VALUES ('{name}', '{gender}');"
    sql += "END;"
    cur.execute(sql)


link = "https://s3-geospatial.s3-us-west-2.amazonaws.com/name_gender.csv"
data = extract(link)
lines = transform(data)
load(lines)