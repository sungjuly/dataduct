#!/usr/bin/env python

"""Replacement for the load step to use the redshift COPY command instead
"""

import argparse
from dataduct.config import get_aws_credentials
from dataduct.data_access import redshift_connection
from dataduct.database import SqlStatement
from dataduct.database import Table
import arrow


def load_redshift(table, input_paths, max_error=0,
                  replace_invalid_char=None, no_escape=False, gzip=False,
                  command_options=None, not_deleted=False):
    """Load redshift table with the data in the input s3 paths
    """
    table_name = table.full_name
    print 'Loading data into %s' % table_name

    # Credentials string
    aws_key, aws_secret, token = get_aws_credentials()
    creds = 'aws_access_key_id=%s;aws_secret_access_key=%s' % (
        aws_key, aws_secret)
    if token:
        creds += ';token=%s' % token

    delete_statement = 'DELETE FROM %s;' % table_name
    error_string = 'MAXERROR %d' % max_error if max_error > 0 else ''
    if replace_invalid_char is not None:
        invalid_char_str = "ACCEPTINVCHARS AS %s" % replace_invalid_char
    else:
        invalid_char_str = ''

    query = []
    if not not_deleted:
        query.append(delete_statement)

    template = \
        "COPY {table} FROM '{path}' WITH CREDENTIALS AS '{creds}' {options};"

    for input_path in input_paths:
        if not command_options:
            command_options = (
                "DELIMITER '\t' {escape} {gzip} NULL AS 'NULL' TRUNCATECOLUMNS "
                "{max_error} {invalid_char_str};"
            ).format(escape='ESCAPE' if not no_escape else '',
                     gzip='GZIP' if gzip else '',
                     max_error=error_string,
                     invalid_char_str=invalid_char_str)

        statement = template.format(table=table_name,
                                    path=input_path,
                                    creds=creds,
                                    options=command_options)
        query.append(statement)
    return ' '.join(query)


def main():
    """Main Function
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('--table_definition', dest='table_definition',
                        required=True)
    parser.add_argument('--max_error', dest='max_error', default=0, type=int)
    parser.add_argument('--replace_invalid_char', dest='replace_invalid_char',
                        default=None)
    parser.add_argument('--no_escape', action='store_true', default=False)
    parser.add_argument('--gzip', action='store_true', default=False)
    parser.add_argument('--not_deleted', action='store_true', default=False)
    parser.add_argument('--command_options', dest='command_options', default=None)
    parser.add_argument('--s3_input_paths', dest='input_paths', nargs='+')
    parser.add_argument('--before_sql', dest='before_sql', default=None)
    parser.add_argument('--sql', dest='sql', default=None)
    parser.add_argument('--after_sql', dest='after_sql', default=None)
    parser.add_argument('--timezone', dest='timezone', default=None)
    parser.add_argument('--time_replace_hour', dest='time_replace_hour', default=0, type=int)
    args = parser.parse_args()
    print args

    args.table_definition = eval(args.table_definition)
    if not isinstance(args.table_definition, list):
        args.table_definition = [args.table_definition]

    connection = redshift_connection()

    if args.before_sql:
        try:
            sql = reserved_keywords(args.timezone, args.time_replace_hour, args.before_sql)
            cursor = connection.cursor()
            cursor.execute(sql)
            cursor.execute('COMMIT')
        except Exception as e:
            print e
            connection.rollback()
            pass
        finally:
            cursor.close()

    index = 0
    input_paths = args.input_paths
    for table_definition in args.table_definition:
        table = Table(SqlStatement(table_definition))
        input_path = [reserved_keywords(args.timezone, args.time_replace_hour, input_paths[index], table.full_name)]
        sql = reserved_keywords(args.timezone, args.time_replace_hour, args.sql, table.full_name)
        load_query = load_redshift(table, input_path, args.max_error,
                                   args.replace_invalid_char, args.no_escape,
                                   args.gzip, args.command_options, args.not_deleted)

        try:
            cursor = connection.cursor()
            # Create table in redshift, this is safe due to the if exists condition
            cursor.execute(table.create_script().sql())
            # Load data into redshift
            cursor.execute(load_query)
            if sql:
                cursor.execute(sql)
            cursor.execute('COMMIT')
        except Exception as e:
            print e
            connection.rollback()
            pass
        finally:
            cursor.close()

        index += 1

    if args.after_sql:
        try:
            sql = reserved_keywords(args.timezone, args.time_replace_hour, args.after_sql)
            cursor = connection.cursor()
            cursor.execute(sql)
            cursor.execute('COMMIT')
        except Exception as e:
            print e
            connection.rollback()
            pass
        finally:
            cursor.close()

    connection.close()


def reserved_keywords(timezone, time_replace_hour, value, table=None):
    if not value:
        return None

    date = arrow.now(tz=timezone)
    date = date.replace(hours=time_replace_hour)
    YYYY = date.format('YYYY')
    MM = date.format('MM')
    DD = date.format('DD')
    HH = date.format('HH')
    mm = date.format('mm')
    ss = date.format('ss')
    return value.format(YYYY=YYYY, MM=MM, DD=DD, HH=HH, mm=mm, ss=ss, table=table)


if __name__ == '__main__':
    main()