#!/usr/bin/env python3
""" Use of regex in replacing occurrences of certain field values """
import re
from typing import List
import logging
import mysql.connector
import os

PII_FIELDS = ("name", "email", "password", "ssn", "phone")  # Module-level tuple


class RedactingFormatter(logging.Formatter):
    """ Redacting Formatter class """
    REDACTION = "***"
    FORMAT = "[HOLBERTON] %(name)s %(levelname)s %(asctime)-15s: %(message)s"
    SEPARATOR = ";"

    def __init__(self, fields: List[str]):
        super(RedactingFormatter, self).__init__(self.FORMAT)
        self.fields = fields

    def format(self, record: logging.LogRecord) -> str:
        """ Returns filtered values from log records """
        return filter_datum(self.fields, self.REDACTION,
                            super().format(record), self.SEPARATOR)


def get_db() -> mysql.connector.connection.MySQLConnection:
    """ Connection to MySQL environment """
    db_connect = mysql.connector.connect(
        user=os.getenv('PERSONAL_DATA_DB_USERNAME', 'root'),
        password=os.getenv('PERSONAL_DATA_DB_PASSWORD', ''),
        host=os.getenv('PERSONAL_DATA_DB_HOST', 'localhost'),
        database=os.getenv('PERSONAL_DATA_DB_NAME')
    )
    return db_connect


def filter_datum(fields: List[str], redaction: str, message: str,
                 separator: str) -> str:
    """ Returns regex obfuscated log messages """
    for field in fields:
        pattern = rf'{field}=[^;{separator}]*'
        message = re.sub(pattern, f'{field}={redaction}', message)
    return message


def get_logger() -> logging.Logger:
    """ Returns a logging.Logger object """
    logger = logging.getLogger("user_data")
    logger.setLevel(logging.INFO)
    logger.propagate = False

    target_handler = logging.StreamHandler()
    target_handler.setLevel(logging.INFO)

    formatter = RedactingFormatter(list(PII_FIELDS))
    target_handler.setFormatter(formatter)

    logger.addHandler(target_handler)
    return logger


def main() -> None:
    """ Obtain database connection using get_db
    retrieve all role in the users table and display
    each row under a filtered format
    """
    db = get_db()
    cursor = db.cursor()
    cursor.execute("SELECT * FROM users;")

    headers = [field[0] for field in cursor.description]
    logger = get_logger()

    for row in cursor:
        message = "; ".join(
            f"{header}={value}" for header, value in zip(headers, row)
        ) + ";"
        logger.info(message)

    cursor.close()
    db.close()


if __name__ == '__main__':
    main()
