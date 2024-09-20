import os
from contextlib import contextmanager

import psycopg2
from sshtunnel import SSHTunnelForwarder


@contextmanager
def ssh_tunnel() -> SSHTunnelForwarder:
    """Create an SSH tunnel to the remote server.

    :return: Yields the tunnel object.
    """
    ssh_host = os.environ.get("SSH_HOST")
    ssh_port = int(os.environ.get("SSH_PORT"))
    ssh_user = os.environ.get("SSH_USER")
    ssh_password = os.environ.get("SSH_PASSWORD")
    db_port = int(os.environ.get("DB_PORT", "5432"))

    tunnel = SSHTunnelForwarder(
        (ssh_host, ssh_port),
        ssh_username=ssh_user,
        ssh_password=ssh_password,
        remote_bind_address=("127.0.0.1", db_port),
    )
    try:
        tunnel.start()
        print(f"SSH tunnel established. Local bind port: {tunnel.local_bind_port}")
        yield tunnel
    finally:
        if tunnel.is_active:
            tunnel.close()
            print("SSH tunnel closed.")


@contextmanager
def connect_to_db():
    """Connect to the PostgreSQL database.

    :return: Yields the database cursor.
    """
    with ssh_tunnel() as tunnel:
        db_host = os.environ.get("DB_HOST", "localhost")
        db_port = tunnel.local_bind_port
        db_name = os.environ.get("DB_NAME")
        db_user = os.environ.get("DB_USER")
        db_password = os.environ.get("DB_PASSWORD")

        conn = psycopg2.connect(
            dbname=db_name,
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port,
        )
        try:
            cur = conn.cursor()
            yield cur
            conn.commit()  # Commit if necessary
        except Exception as e:
            conn.rollback()
            print(f"Database operation error: {e}")
            raise
        finally:
            cur.close()
            conn.close()
