import psycopg2

def get_Redshift_connection():
    host = "learnde.cduaw970ssvt.ap-northeast-2.redshift.amazonaws.com"
    redshift_user = "user"  
    redshift_pass = "password"
    port = 5439
    dbname = "dev"
    conn = psycopg2.connect(f"dbname={dbname} user={redshift_user} host={host} password={redshift_pass} port={port}")
    conn.set_session(autocommit=True)
    return conn.cursor()


cur = get_Redshift_connection()
sql = '''
WITH RECURSIVE active_users AS (
  SELECT TO_CHAR(st.ts, 'yyyy-mm') AS year_month, usc.userid AS userid
  FROM raw_data.session_timestamp st
  JOIN raw_data.user_session_channel usc
  ON st.sessionid = usc.sessionid)

SELECT year_month, COUNT(DISTINCT userid) AS user_count FROM active_users
GROUP BY year_month
ORDER BY year_month;
'''
cur.execute(sql)
for row in cur.fetchall():
    print(row)




