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

# Gross Revenue가 가장 큰 UserID 10개 찾기
sql = '''
SELECT userid, sum(amount) AS total_amount
FROM raw_data.user_session_channel usc
JOIN raw_data.session_transaction st ON usc.sessionid = st.sessionid
GROUP BY userid
ORDER BY total_amount DESC
LIMIT 10;
'''
cur.execute(sql)
for row in cur.fetchall():
    print(row)
