import pandas as pd
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
SELECT TO_CHAR(st.ts, 'yyyy-mm') AS year_month, usc.userid AS userid
FROM raw_data.session_timestamp st
JOIN raw_data.user_session_channel usc
ON st.sessionid = usc.sessionid
'''
cur.execute(sql)

df_active_user = pd.DataFrame(cur.fetchall(),  columns = ['year_month', 'userid'])

df_monthly_active_user = df_active_user.groupby('year_month')['userid'].nunique()
print(df_monthly_active_user)




