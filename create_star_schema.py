import psycopg2

init_q = '''DROP TABLE IF EXISTS student_ath.academic_score_snapshot_fact,
                                student_ath.location_dim,
                                student_ath.school_dim,
                                student_ath.sport_dim,
                                student_ath.date_dim;
            DROP SCHEMA IF EXISTS student_ath;
            CREATE SCHEMA IF NOT EXISTS student_ath
            AUTHORIZATION postgres;'''

create_date_dim_q = '''
    CREATE  TABLE student_ath.date_dim ( 
        date_key             integer Primary key,
        year                 integer   
);
'''

create_location_dim_q = '''
    CREATE  TABLE student_ath.location_dim ( 
        location_key         integer Primary key ,
        state                varchar(100)   ,
        city                 varchar(100)   
);
'''

create_school_dim_q = '''
    CREATE  TABLE student_ath.school_dim ( 
	   school_key           integer Primary key ,
	   school_name          varchar(100)   ,
	   school_conference    varchar(100)   ,
	   school_type          varchar   
);
'''

create_sport_dim_q = '''
    CREATE  TABLE student_ath.sport_dim ( 
	   sport_key            integer Primary key ,
	   sport           varchar(100)   ,
	   gender         varchar(5)   ,
	   contact_sport        char(1)   
 );
'''

create_fact_tbl_q = '''
    CREATE  TABLE student_ath.academic_score_snapshot_fact ( 
	   date_key             integer  ,
	   location_key         integer   ,
	   sport_key            integer   ,
	   school_key           integer   ,
	   academic_score       integer   ,
	   num_athletes         integer   ,
	   PRIMARY KEY (date_key, location_key, sport_key, school_key),
	   FOREIGN KEY ( date_key ) REFERENCES student_ath.date_dim( date_key )   ,
	   FOREIGN KEY ( location_key ) REFERENCES student_ath.location_dim( location_key )   ,
	   FOREIGN KEY ( school_key ) REFERENCES student_ath.school_dim( school_key )   ,
	   FOREIGN KEY ( sport_key ) REFERENCES student_ath.sport_dim( sport_key )   
 );
'''

queries = [init_q, create_date_dim_q, create_location_dim_q, create_school_dim_q, create_sport_dim_q, create_fact_tbl_q]

db_name = "student_ath_academics"
username = "postgres"
password = "banana10()"

conn = psycopg2.connect(host = "localhost",
                        dbname = db_name,
                        user = username,
                        password = password)

cur = conn.cursor()

for query in queries:
    cur.execute(query)
    conn.commit()

if(conn):
    conn.close()
    cur.close()