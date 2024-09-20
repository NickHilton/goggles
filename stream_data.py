import argparse
import io

import config
import database


def stream_csv_to_postgres(filepath: str, table:str | None) -> None:
    """Stream CSV data to a PostgreSQL table.

    :param filepath: name of local file to stream
    :param table: table to stream data to
    :return: None - Streams data to PostgreSQL table
    """

    # Wrap the raw HTTP response in a TextIOWrapper to get a file-like object
    with open(filepath, "rb") as file:
        csv_stream = io.TextIOWrapper(file, encoding="utf-8")
        with database.connect_to_db() as cur:
            # Use PostgreSQL's COPY command for fast bulk insertion
            insert_table = table or config.INSERT_TABLE
            copy_sql = (
                f"COPY {insert_table} FROM STDIN WITH CSV HEADER DELIMITER ','"
            )
            cur.copy_expert(sql=copy_sql, file=csv_stream)
            print("Data has been successfully inserted into the PostgreSQL table.")


def parse_args():
    parser = argparse.ArgumentParser(description="Stream CSV data to PostgreSQL")
    parser.add_argument("--filepath", type=str, help="Path to the CSV file")
    parser.add_argument("--table", type=str, help="Name of the table to insert data into")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    stream_csv_to_postgres(filepath=args.filepath, table=args.table)
