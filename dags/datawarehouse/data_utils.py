from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import RealDictCursor
from psycopg2 import sql

TABLE = "yt_api"

def get_conn_cursor():
    hook = PostgresHook(
        postgres_conn_id="postgres_db_yt_elt",
        database="elt_db"
    )

    conn = hook.get_conn()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    return conn, cur


def close_conn_cursor(conn, cur):
    # cur: permite ejecutar instrucciones SQL y recuperar resultados utilizando esa conexión.
    cur.close()
    # conn: establece y mantiene la comunicación con PostgreSQL.
    conn.close()


def create_schema(schema):

    conn, cur = get_conn_cursor()

    try:

        query = sql.SQL(
            "CREATE SCHEMA IF NOT EXISTS {}"
        ).format(
            sql.Identifier(schema)
        )

        cur.execute(query)

        conn.commit()

    except Exception:

        conn.rollback()
        raise

    finally:

        close_conn_cursor(conn, cur)


def create_table(schema):

    if schema not in ("staging", "core"):
        raise ValueError("El esquema debe ser staging o core")

    conn, cur = get_conn_cursor()

    try:

        if schema == "staging":

            columns = """
                "Video_ID" VARCHAR(11) PRIMARY KEY,
                "Video_Title" TEXT NOT NULL,
                "Upload_Date" TIMESTAMPTZ NOT NULL,
                "Duration" VARCHAR(20) NOT NULL,
                "Video_Views" BIGINT,
                "Likes_Count" BIGINT,
                "Comments_Count" BIGINT
            """

        else:

            columns = """
                "Video_ID" VARCHAR(11) PRIMARY KEY,
                "Video_Title" TEXT NOT NULL,
                "Upload_Date" TIMESTAMPTZ NOT NULL,
                "Duration" INTERVAL NOT NULL,
                "Video_Type" VARCHAR(10) NOT NULL,
                "Video_Views" BIGINT,
                "Likes_Count" BIGINT,
                "Comments_Count" BIGINT
            """

        query = sql.SQL(
            "CREATE TABLE IF NOT EXISTS {}.{} ({})"
        ).format(
            sql.Identifier(schema),
            sql.Identifier(TABLE),
            sql.SQL(columns)
        )

        cur.execute(query)

        conn.commit()

    except Exception:

        conn.rollback()
        raise

    finally:

        close_conn_cursor(conn, cur)


def get_video_ids(cur, schema):

    query = sql.SQL(
        'SELECT "Video_ID" FROM {}.{}'
    ).format(
        sql.Identifier(schema),
        sql.Identifier(TABLE)
    )

    cur.execute(query)

    ids = cur.fetchall()

    return [row["Video_ID"] for row in ids]


  