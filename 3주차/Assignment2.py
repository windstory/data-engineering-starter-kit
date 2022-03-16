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

# 사용자별로 처음/마지막 채널 알아내기
# using ROW_NUMBER
sql_with_ROW_NUMBER = '''
WITH RECURSIVE channel_seq_table AS(
SELECT userid, channel, ROW_NUMBER() OVER (PARTITION BY userid ORDER BY ts) AS channel_seq
FROM raw_data.user_session_channel usc
JOIN raw_data.session_timestamp st ON usc.sessionid = st.sessionid)

SELECT a.userid, cst1.channel AS first_channel, cst2.channel AS last_channel
FROM (SELECT userid, MAX(channel_seq) AS max_value FROM channel_seq_table GROUP BY userid) a
JOIN channel_seq_table cst1 ON a.userid = cst1.userid AND cst1.channel_seq = 1
JOIN channel_seq_table cst2 ON a.userid = cst2.userid AND a.max_value = cst2.channel_seq
ORDER BY a.userid;
'''
# using FIRST_VALUE / LAST_VALUE
sql_with_FIRST_LAST_VALUE = '''
SELECT DISTINCT userid, 
    FIRST_VALUE(channel) OVER (PARTITION BY userid ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING  AND UNBOUNDED FOLLOWING) AS first_channel, 
    LAST_VALUE(channel) OVER (PARTITION BY userid ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING  AND UNBOUNDED FOLLOWING) AS last_channel
FROM raw_data.user_session_channel usc
JOIN raw_data.session_timestamp st ON usc.sessionid = st.sessionid
ORDER BY userid;
'''

cur.execute(sql_with_FIRST_LAST_VALUE)
for row in cur.fetchall():
    print(row)
