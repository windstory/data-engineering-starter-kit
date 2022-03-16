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

# 채널별 월 매출액 테이블 만들기
sql = '''
DROP TABLE IF EXISTS windstoryya.monthly_channel_revenue_summary;
CREATE TABLE windstoryya.monthly_channel_revenue_summary AS
SELECT TO_CHAR(ts, 'YYYY-mm') AS "year-month", channel, count(DISTINCT userid) AS uniqueUsers,
    count(DISTINCT CASE WHEN amount > 0 THEN userid END) AS paidUsers,
    ROUND(paidUsers::decimal*100 / NULLIF(uniqueUsers, 0), 2) AS conversionRate,
    sum(amount) AS grossRevenue,
    sum(CASE WHEN refunded is FALSE THEN amount ELSE 0 END) AS netRevenue
FROM raw_data.user_session_channel usc
LEFT JOIN raw_data.session_timestamp timestamp ON usc.sessionid = timestamp.sessionid
LEFT JOIN raw_data.session_transaction transaction ON usc.sessionid = transaction.sessionid
GROUP BY "year-month", channel
ORDER BY "year-month";
'''
cur.execute(sql)
for row in cur.fetchall():
    print(row)
