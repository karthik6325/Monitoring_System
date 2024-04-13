from fastapi import FastAPI, HTTPException
from sqlalchemy.orm import Session
from database import SessionLocal, engine
from models import Report
from sqlalchemy import  text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import uuid
import csv
from io import StringIO
import threading


DATABASE_URL = "postgresql://postgres:lavkar@localhost/postgres"

app = FastAPI()


report_status = {}


async def trigger_report():
    try:
        session = SessionLocal()
        unique_table_name = f"report_{str(uuid.uuid4()).replace('-', '_')}"
        print(unique_table_name)
        report_status[unique_table_name] = True

        threading.Thread(target=execute_report_query, args=(session, unique_table_name)).start()


        return "Report generation triggered"
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        session.close()



async def execute_report_query(session: Session, unique_table_name):
    try:
        report_status[unique_table_name] = True
        sql_query = text(
                f"""
                CREATE TABLE IF NOT EXISTS {unique_table_name} (
                store_id BIGINT,
                uptime_last_hour_minutes INT,
                uptime_last_day_hours FLOAT,
                uptime_last_week FLOAT,
                downtime_last_day_hours FLOAT,
                downtime_last_hour_minutes INT,
                downtime_last_week FLOAT
            );

            WITH all_store_ids AS (
                SELECT store_id FROM public."Store_Status"
                UNION
                SELECT store_id FROM public."Store_Activity"
                UNION
                SELECT store_id FROM public."Store_Time_Zone"
            ),
            store_activity_utc AS (
                SELECT DISTINCT
                    asi.store_id,
                    gs.day AS day,
                    COALESCE(
                        (sa.start_time_local AT TIME ZONE COALESCE(stz.timezone_str, 'America/Chicago'))::time,
                        '00:00:00'::time
                    ) AS start_time_utc,
                    COALESCE(
                        (sa.end_time_local AT TIME ZONE COALESCE(stz.timezone_str, 'America/Chicago'))::time,
                        '23:59:59'::time
                    ) AS end_time_utc
                FROM
                    all_store_ids asi
                CROSS JOIN
                    generate_series(0, 6) AS gs(day)
                LEFT JOIN
                    public."Store_Activity" sa ON asi.store_id = sa.store_id AND gs.day = sa.day
                LEFT JOIN
                    public."Store_Time_Zone" stz ON asi.store_id = stz.store_id
            ),
            max_timestamp AS (
                SELECT TIMESTAMP '2023-01-18 18:13:22.47922' AS target_timestamp
            ),
            filtered_store_status AS (
                SELECT *
                FROM public."Store_Status"
                WHERE timestamp_utc > (SELECT target_timestamp FROM max_timestamp)
            ),
            total_uptime_per_day AS (
                SELECT
                    sa.store_id,
                    sa.day,
                    SUM(
                        CASE 
                            WHEN sa.end_time_utc < sa.start_time_utc THEN 
                                EXTRACT(EPOCH FROM (sa.end_time_utc - sa.start_time_utc) + INTERVAL '1 DAY') / 3600 -- convert minutes to hours
                            ELSE
                                EXTRACT(EPOCH FROM (LEAST(sa.end_time_utc, '23:59:59'::time) - GREATEST(sa.start_time_utc, '00:00:00'::time))) / 3600 -- convert minutes to hours
                        END
                    ) AS total_uptime_hours
                FROM
                    store_activity_utc sa
                GROUP BY
                    sa.store_id, sa.day
            ),
            valid_timestamps AS (
                SELECT DISTINCT ON (ss.store_id, sa.day)
                    ss.store_id,
                    ss.timestamp_utc AS valid_timestamp,
                    (EXTRACT(ISODOW FROM ss.timestamp_utc) - 1) AS day, -- Adjust day of week
                    ss.status,
                    sa.start_time_utc,
                    sa.end_time_utc
                FROM
                    filtered_store_status ss
                JOIN
                    store_activity_utc sa ON ss.store_id = sa.store_id
                WHERE
                    ss.timestamp_utc::time BETWEEN sa.start_time_utc AND sa.end_time_utc
                ORDER BY
                    ss.store_id, sa.day, ss.timestamp_utc -- Ensure uniqueness
            ),
            uptime_downtime_per_day AS (
                SELECT
                    tu.store_id,
                    tu.day,
                    tu.start_time_utc,
                    tu.end_time_utc,
                    COALESCE(tup.total_uptime_hours, 0) AS uptime_hours
                FROM
                    (
                        SELECT DISTINCT store_id, day, start_time_utc, end_time_utc
                        FROM store_activity_utc
                    ) AS tu
                LEFT JOIN
                    total_uptime_per_day tup ON tu.store_id = tup.store_id AND tu.day = tup.day
            ),
            status_counts AS (
                SELECT
                    sa.store_id,
                    sa.day,
                    COALESCE(SUM(CASE WHEN vt.status IS NOT NULL THEN 1 ELSE 0 END), 0) AS total_entries,
                    COALESCE(SUM(CASE WHEN vt.status = 'inactive' THEN 1 ELSE 0 END), 0) AS cnt_inactive

                FROM
                    store_activity_utc sa
                LEFT JOIN
                    valid_timestamps vt ON sa.store_id = vt.store_id AND sa.day = vt.day AND sa.start_time_utc = vt.start_time_utc AND sa.end_time_utc = vt.end_time_utc
                GROUP BY
                    sa.store_id, sa.day
            ),
            last_status_per_day AS (
                SELECT
                    sa.store_id,
                    sa.day,
                    LAST_VALUE(vt.status) OVER (PARTITION BY sa.store_id, sa.day ORDER BY vt.valid_timestamp DESC) AS last_status,
                    LAST_VALUE(vt.valid_timestamp) OVER (PARTITION BY sa.store_id, sa.day ORDER BY vt.valid_timestamp DESC) AS last_status_timestamp
                FROM
                    store_activity_utc sa
                LEFT JOIN
                    valid_timestamps vt ON sa.store_id = vt.store_id AND sa.day = vt.day AND sa.start_time_utc = vt.start_time_utc AND sa.end_time_utc = vt.end_time_utc
            ),
            uptime_last_hour AS (
                SELECT
                    asi.store_id,
                    CASE
                        WHEN lspd.last_status IS NULL THEN 0 -- No data for last hour, downtime is 60, uptime is 0
                        WHEN lspd.last_status = 'active' THEN 60 -- Last status was active, uptime is 60, downtime is 0
                        ELSE 0 -- Last status was inactive, uptime is 0, downtime is 60
                    END AS uptime_last_hour,
                    CASE
                        WHEN lspd.last_status IS NULL THEN 60
                        ELSE 0
                    END AS downtime_last_hour
                FROM
                    all_store_ids asi
                LEFT JOIN
                    last_status_per_day lspd ON asi.store_id = lspd.store_id AND EXTRACT(DOW FROM CURRENT_DATE)::int = lspd.day -- Adjust for zero-based indexing of days
            ),
            uptime_last_day AS (
            SELECT
                sc.store_id,
                sc.day,
                COALESCE(udp.total_uptime_hours, 0) AS uptime_hours,
                CASE
                WHEN sc.total_entries = 0 THEN
                    0 -- Assuming 24 hours in a day since there are no entries
                WHEN sc.cnt_inactive = sc.total_entries THEN
                    24 -- All entries are inactive, downtime is 24 hours
                ELSE
                    24 - COALESCE(udp.total_uptime_hours, 0) -- Subtract uptime hours from 24 hours
                END AS downtime_last_day_hours
            FROM
                status_counts sc
            LEFT JOIN
                total_uptime_per_day udp ON sc.store_id = udp.store_id AND sc.day = udp.day
            ),
            total_uptime_per_week AS (
            SELECT
                store_id,
                SUM(total_uptime_hours) AS total_uptime_week
            FROM
                total_uptime_per_day
            GROUP BY
                store_id
            ),
            uptime_downtime_per_week AS (
            SELECT
                asi.store_id,
                COALESCE(SUM(CASE WHEN vt.status = 'active' THEN COALESCE(udp.total_uptime_hours, 0) ELSE (total_uptime_week - COALESCE(udp.total_uptime_hours, 0)) END), 0) AS total_uptime_week,
                COALESCE(SUM(CASE WHEN vt.status = 'inactive' THEN COALESCE(udp.total_uptime_hours, 0) ELSE 0 END), 0) AS total_downtime_week
            FROM
                all_store_ids asi
            LEFT JOIN
                total_uptime_per_week tupw ON asi.store_id = tupw.store_id
            LEFT JOIN
                valid_timestamps vt ON asi.store_id = vt.store_id
            LEFT JOIN 
                total_uptime_per_day udp ON asi.store_id = udp.store_id
            GROUP BY
                asi.store_id, total_uptime_week
            )
            INSERT INTO {unique_table_name}
            SELECT DISTINCT ON (sc.store_id)
                sc.store_id,
                ulh.uptime_last_hour AS uptime_last_hour_minutes,
                uld.uptime_hours AS uptime_last_day_hours,
                udw.total_uptime_week/60 AS uptime_last_week,
                uld.downtime_last_day_hours,
                ulh.downtime_last_hour AS downtime_last_hour_minutes,
                udw.total_downtime_week/60 AS downtime_last_week
            FROM
                status_counts sc
            LEFT JOIN
                uptime_last_hour ulh ON sc.store_id = ulh.store_id
            LEFT JOIN
                uptime_last_day uld ON sc.store_id = uld.store_id AND sc.day = uld.day
            LEFT JOIN
                uptime_downtime_per_week udw ON sc.store_id = udw.store_id;

                """
            )
        result = session.execute(sql_query)
        session.commit()
        # print(result)
        report_status[unique_table_name] = False
    
    except Exception as e:
        print(f"Error executing report query: {e}")
    finally:
        session.close()



async def get_report(report_id: int):
    try:
        if report_id in report_status and report_status[report_id]:
            return {"status": "running"}

        table_name = f"{report_id}"
        Session = sessionmaker(autocommit=False, autoflush=False, bind=engine)
        session = Session()
        report_data = session.query(Report).from_statement(text(f"SELECT * FROM {table_name}")).all()
        session.close()

        csv_output = StringIO()
        csv_writer = csv.writer(csv_output)
        csv_writer.writerow(["store_id", "uptime_last_hour_minutes", "uptime_last_day_hours", "uptime_last_week", "downtime_last_day_hours", "downtime_last_hour_minutes", "downtime_last_week"])
        for row in report_data:
            csv_writer.writerow([row.store_id, row.uptime_last_hour_minutes, row.uptime_last_day_hours, row.uptime_last_week, row.downtime_last_day_hours, row.downtime_last_hour_minutes, row.downtime_last_week])

        csv_output.seek(0)
        return report_data
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


