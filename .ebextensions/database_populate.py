#!/usr/bin/env python
import sys
import pymysql
import csv
import os

airport_sql = """
create table airport (
ident nvarchar(10),
type nvarchar(50),
name nvarchar(200),
latitude float,
longitude float,
elevation_ft float,
continent nvarchar(10),
iso_country nvarchar(10),
iso_region nvarchar(10),
municipality nvarchar(100),
gps_code nvarchar(10),
iata_code nvarchar(10),
local_code nvarchar(10)
);
"""

city_sql = """
create table city (
	id nvarchar(200),
	name nvarchar(200),
	icon VARCHAR(191) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
	latitude float,
	longitude float,
	CONSTRAINT pk_city PRIMARY KEY(id)
);
"""

with open('.ebextensions/airport-codes.csv','rb') as csvfile: 
    reader = csv.DictReader(csvfile)
    airports = list(reader)
airports = filter(lambda x: x['type'] == 'large_airport', airports)

with open('.ebextensions/cities.csv','rb') as csvfile: 
    reader = csv.DictReader(csvfile, delimiter='\t')
    cities = list(reader)


#rds settings
rds_host  = os.environ['DATABASE_HOST']
db_user = os.environ['DATABASE_USER']
password = os.environ['DATABASE_PASSWORD']
db_name = os.environ['DATABASE_DB_NAME']
port = 3306

server_address = (rds_host, port)
conn = pymysql.connect(rds_host, user=db_user, passwd=password, db=db_name, connect_timeout=5, charset='utf8mb4')
cursor = conn.cursor()

cursor.execute("SHOW TABLES LIKE 'city'")
result = cursor.fetchone()
if not result:
    print "Creating city table"
    conn.cursor().execute(city_sql)
    conn.commit()
    
    print "Populating city table"
    for city in cities:
        sql = """INSERT INTO `city` (`id`,`name`,`icon`,`latitude`,`longitude`) VALUES (%s,%s,%s,%s,%s);"""
        try:
            with conn.cursor() as cur:
                cur.execute(sql, (city["ID"], city["City"], city["Icon"], city['Latitude'], city['Longitude']))
                conn.commit()
        except:
            print("Unexpected error! ", sys.exc_info())
            sys.exit("Error!")

cursor.execute("SHOW TABLES LIKE 'airport'")
result = cursor.fetchone()
if not result:
    print "Creating airport table"
    conn.cursor().execute(airport_sql)
    conn.commit()
    
    print "Populating airport table"
    for airport in airports:

        sql = """
        INSERT INTO `airport` (ident,type,name,latitude,longitude,elevation_ft,continent,iso_country,iso_region,municipality,gps_code,iata_code,local_code)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s); 
        """
        try:
            with conn.cursor() as cur:
                cur.execute(sql, ( airport["ident"], airport["type"], airport["name"], airport["latitude_deg"], airport["longitude_deg"], airport["elevation_ft"], airport["continent"], airport["iso_country"], airport["iso_region"], airport["municipality"], airport["gps_code"], airport["iata_code"], airport["local_code"]  ))
                conn.commit()
        except:
            print("Unexpected error! ", sys.exc_info())
            sys.exit("Error!")


conn.close()
