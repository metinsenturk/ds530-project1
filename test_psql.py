import decimal
import psycopg2 as psy
from psycopg2.extras import RealDictCursor, DictCursor, NamedTupleCursor
import json
import csv


def decimal_default(obj):
    if isinstance(obj, decimal.Decimal):
        return float(obj)
    raise TypeError


# connection
s = "host='localhost' user='postgres' dbname='zagi'"
psy_conn = psy.connect(s)

print(psy_conn.get_dsn_parameters())
print(psy_conn.closed)

# cursor
cursor = psy_conn.cursor(cursor_factory=RealDictCursor)
print(cursor.closed)
print(cursor.name)
print(cursor.description)

# cursor operations in db
cursor.execute("select version();")
cursor.execute("select * from product;")
rows = cursor.fetchone()
print(json.dumps(rows, default=decimal_default))

print("---------------------------------")

cursor2 = psy_conn.cursor(cursor_factory=NamedTupleCursor)
cursor2.execute("select * from product;")
rows = cursor2.fetchone()
print(rows)

print("---------------------------------")

cursor3 = psy_conn.cursor(cursor_factory=DictCursor)
cursor3.execute("select * from product;")
rows = cursor3.fetchone()
print(rows)

print("---------------------------------")

cursor4 = psy_conn.cursor()
cursor4.execute("select * from product;")
rows = cursor4.fetchall()
print(rows)

with open("product.csv", 'w') as f:
    writer = csv.writer(f, delimiter=',')
    for row in rows:
        writer.writerow(row)

# closing cursor and connection
cursor.close()
cursor2.close()
cursor3.close()
cursor4.close()
psy_conn.close()
print(cursor.closed)
print(psy_conn.closed)
