import os
from sqlalchemy import create_engine
from sqlalchemy.sql import text
import time


from dotenv import load_dotenv

load_dotenv()

MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASSWD = os.getenv("MYSQL_PASSWD")
MYSQL_DB = os.getenv('DB_NAME', 'rightmove')


def get_engine():
    engine = create_engine(
        'mysql+mysqldb://{user}:{passwd}@{host}/{db}'.format(user=MYSQL_USER, passwd=MYSQL_PASSWD, host=MYSQL_HOST, db=MYSQL_DB),echo=True, connect_args={'ssl':'DISABLED'})
    return engine

def time_consumption(begin,end):
    spend = end -begin
    convert_str = time.strftime("%H:%M:%S", time.gmtime(spend))
    return convert_str


if __name__ == "__main__":
    begin = time.time()
    engine = get_engine()
    stmt = text(
        "delete from daily_rent "
        "where date < CURRENT_DATE - INTERVAL 2 MONTH"
        )
    test = text(
        "select count(*) from daily_rent"
        )
    with engine.connect() as connection:
        result = connection.execute(stmt)
        print(result.first())
    time_spend = time_consumption(begin,time.time())
    print("Finish! spend time:",time_spend, "and today is:",time.gmtime())

