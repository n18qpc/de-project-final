def get_config(conn):
    conn_config = {
            "user": conn.login, 
            "password": conn.password,
            "database": conn.schema,
            "autocommit": True,
            "host": conn.host
        }
    return conn_config