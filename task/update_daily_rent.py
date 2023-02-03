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
        'mysql+mysqldb://{user}:{passwd}@{host}/{db}'.format(user=MYSQL_USER, passwd=MYSQL_PASSWD, host=MYSQL_HOST, db=MYSQL_DB))
    return engine


if __name__ == "__main__":
    begin = time.time()
    engine = get_engine()
    stmt = text(
        "delete from daily_rent "
        "where date < CURRENT_DATE - INTERVAL 2 MONTH"
        )
    with engine.connect() as connection:
        result = connection.execute(stmt)
        print(result)
    print("finish ",time.time()-begin)

