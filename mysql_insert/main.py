import time
import mysql.connector
from mysql.connector import Error
from faker import Faker
import prettytable

Faker.seed(0)
fake = Faker()

create_people_sql = """
CREATE TABLE `people` (
  `id` int NOT NULL AUTO_INCREMENT,
  `first_name` varchar(50) COLLATE utf8_unicode_ci NOT NULL,
  `last_name` varchar(50) COLLATE utf8_unicode_ci NOT NULL,
  `email` varchar(100) COLLATE utf8_unicode_ci NOT NULL,
  `city` varchar(100) COLLATE utf8_unicode_ci NOT NULL,
  `country` varchar(100) COLLATE utf8_unicode_ci NOT NULL,
  `phone_number` varchar(25) COLLATE utf8_unicode_ci NOT NULL, 
  `birthdate` date NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
"""

create_paragraphs_table = """
CREATE TABLE `paragraphs` (
    `id` int NOT NULL AUTO_INCREMENT,
    `paragraph` varchar(4000) COLLATE utf8_unicode_ci NOT NULL,
    PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
"""

create_geo_table = """
CREATE TABLE `locations` (
    `id` int NOT NULL AUTO_INCREMENT,
    `coords` geometry NOT NULL SRID 4326,
    `city` varchar(255) UNIQUE NOT NULL,
    `country` varchar(5) NOT NULL,
    `continent` varchar(15) NOT NULL,
    `capital` varchar(50) NOT NULL,
    PRIMARY KEY (`id`),
    SPATIAL KEY `g` (`coords`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
"""
def create_table(cursor, sql_statement):
    try:
        cursor.execute(sql_statement)
        print("Table created")
    except Exception as e:
        pass
        # print("Error creating table", e)
def populate_tables(cursor):
    cursor.execute("SELECT * FROM paragraphs;")
    if not cursor.fetchall():
        n = 0
        while n<10000:
            try:
                n += 1
                # insert data into table
                cursor.execute(' \
                                INSERT INTO `paragraphs` (paragraph) \
                                VALUES ("%s"); \
                                ' % (fake.paragraph()))

                if n % 100 == 0:
                    print("iteration %s" % n)
                    time.sleep(0.5)
                    conn.commit()
            except Exception as e:
                pass

    cursor.execute("SELECT * FROM people;")
    if not cursor.fetchall():
            row = {}
            n = 0
            while n < 100000:
                try:
                    n += 1
                    # define data to be inserted
                    row = [fake.first_name(), fake.last_name(), fake.email(),
                           fake.city(), fake.country(), fake.country_calling_code() + fake.msisdn(), fake.date_of_birth()]

                    add_person = ("INSERT INTO people "
                                   "(first_name, last_name, email, city, country, phone_number, birthdate)"
                                   "VALUES (%s, %s, %s, %s, %s, %s, %s)")
                    data_person = (row[0], row[1], row[2], row[3], row[4], row[5], row[6])

                    cursor.execute(add_person, data_person)

                    if n % 100 == 0:
                        print("iteration %s" % n)
                        time.sleep(0.5)
                        conn.commit()
                except Exception as e:
                    print(e)
                    # pass

    cursor.execute("SELECT * FROM locations;")
    if not cursor.fetchall():
        n = 0
        while n < 1000:
            try:
                # define data to be inserted
                row = fake.location_on_land()
                split = row[4].split('/')
                continent = split[0]
                capital = split[1]

                # insert data into table
                cursor.execute(' \
                       INSERT INTO `locations` (coords, city, country, continent, capital) \
                       VALUES (ST_SRID(POINT("%s","%s"), 4326), "%s", "%s", "%s", "%s"); \
                       ' % (row[1], row[0], row[2], row[3], continent, capital))
                n += 1


                if n % 100 == 0:
                    print("iteration %s" % n)
                    time.sleep(0.5)
                    conn.commit()

            except Exception as e:
                pass

def print_tabular(cursor, results):
    header = []
    for cd in cursor.description:
        header.append(cd[0])

    table = prettytable.PrettyTable(header)
    for row in results:
        table.add_row(row)

    print(table)
def execute_query(cursor):
    try:
        query = input("Insert query here:")
        start_time = time.time()
        cursor.execute(query)
        end_time = time.time()
        execution_time = end_time - start_time
        print("Execution time: " + str(execution_time))
        split = query.split()

        if split[0].lower() == "explain" or split[0].lower() == "select":
            while True:
                opt = input("Do you want to print results? (Y/N): ")
                if opt!="":
                    break
            if opt[0].lower() == "y":
                results = cursor.fetchall()
                if results:
                    print_tabular(cursor, results)

    except Exception as e:
        print("Error executing query ", e)

try:
    conn = mysql.connector.connect(host='localhost', database='bazepodataka',
                                   user='root', password='')

    if conn.is_connected():
        cursor = conn.cursor()

        try:
            cursor.execute("SET @innodb_adaptive_hash_index:='OFF'")
        except:
            #has to be disabled so it doesn't affect query execution time approximation
            print("Error disabling adaptive hash index")

        create_table(cursor, create_people_sql)
        create_table(cursor, create_geo_table)
        create_table(cursor, create_paragraphs_table)

        populate_tables(cursor)

        opt = "Y"
        while opt[0] == "Y" or opt[0] == "y":
            execute_query(cursor)
            opt = input("Do you want to continue executing queries? (Y/N): ")
            if opt == "":
                break
            opt = opt.strip()

        print("Exiting...")

except Error as e :
    print ("error", e)
    pass
except Exception as e:
    print ("Unknown error %s", e)
finally:
    if(conn and conn.is_connected()):
        conn.commit()
        cursor.close()
        conn.close()