import requests
import sqlite3
from datetime import datetime

def create_connection(db_file):
    """ create a database connection to the SQLite database
        specified by db_file
    :param db_file: database file
    :return: Connection object or None
    """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except:
        print('connect to sqlite3 failed')

    return conn

def create_table(conn, create_table_sql):
    """ create a table from the create_table_sql statement
    :param conn: Connection object
    :param create_table_sql: a CREATE TABLE statement
    :return:
    """
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except:
        print('create table failed')

def create_user_address_transaction_tables(conn):
    """ create user, address and transaction 3 tables
    :param conn: Connection object
    :return:
    """
    sql_create_users_table = """ CREATE TABLE IF NOT EXISTS users (
                                                id integer PRIMARY KEY,
                                                username text NOT NULL,
                                                first_name text,
                                                last_name text,
                                                email text,
                                                phone text
                                            ); """

    sql_create_addresses_table = """ CREATE TABLE IF NOT EXISTS addresses (
                                                id integer PRIMARY KEY,
                                                address text NOT NULL,
                                                user_id text NOT NULL,
                                                balance text,
                                                last_synced_time text
                                            ); """

    sql_create_transactions_table = """ CREATE TABLE IF NOT EXISTS transactions (
                                                id integer PRIMARY KEY,
                                                transaction_hash text,
                                                from_address text NOT NULL,
                                                to_address text NOT NULL,
                                                user_id text NOT NULL
                                            ); """

    # create tables
    if conn is not None:
        # create user table
        create_table(conn, sql_create_users_table)
        # create address table
        create_table(conn, sql_create_addresses_table)
        # create transaction table
        create_table(conn, sql_create_transactions_table)
    else:
        print("Error! cannot create the database connection.")

def create_user(conn, user):
    """
    Create a new user
    :param conn:
    :param user:
    :return:
    """

    sql = ''' INSERT INTO users(username,first_name,last_name,email,phone)
              VALUES(?,?,?,?,?) '''
    cur = conn.cursor()
    cur.execute(sql, user)
    conn.commit()
    return cur.lastrowid

def create_address(conn, address):
    """
    Create a new user
    :param conn:
    :param user:
    :return:
    """

    sql = ''' INSERT INTO addresses(address,user_id,balance,last_synced_time)
              VALUES(?,?,?,?) '''
    cur = conn.cursor()
    cur.execute(sql, address)
    conn.commit()
    return cur.lastrowid

def create_transaction(conn, transaction):
    """
    Create a new user
    :param conn:
    :param user:
    :return:
    """

    sql = ''' INSERT INTO transactions(transaction_hash,from_address,to_address,user_id)
              VALUES(?,?,?,?) '''
    cur = conn.cursor()
    cur.execute(sql, transaction)
    conn.commit()
    return cur.lastrowid


def get_current_balance_and_historical_transactions(conn, address):
    """
    View the current balance and historical transactions for given address
    :param conn:
    :param address:
    :return:
    """
    balance = get_current_balance(conn, address)[0]
    historical_transactions = get_current_historical_transactions(conn, address)
    return balance, historical_transactions

def get_current_balance(conn, address):
    """
    Get current balance give an address
    :param conn:
    :param address:
    :return:
    """
    cur = conn.cursor()
    cur.execute("SELECT balance FROM addresses WHERE address=?", (address,))
    rows = cur.fetchall()
    return rows


def get_current_historical_transactions(conn, address):
    """
    Get current historical transactions given address
    :param conn: the Connection object
    :param address:
    :return:
    """
    cur = conn.cursor()
    cur.execute("SELECT * FROM transactions WHERE from_address=? or to_address=?", (address,address))

    rows = cur.fetchall()
    return rows


def retrieve_latest_balance(address):
    url = f'https://api.blockchair.com/bitcoin/dashboards/address/{address}'
    response = requests.get(url)
    data = response.json()['data']
    balance = data[address]['address']['balance']
    # transactions = data[address]['transactions']
    return balance


def retrieve_latest_transactions_details(transaction_hashes, user_id):
    transactions = []
    for transaction_hash in transaction_hashes:
        url = f'https://api.blockchair.com/bitcoin/dashboards/transaction/{transaction_hash}'
        data = requests.get(url).json()['data']
        from_address = data[transaction_hash]['inputs'][0]['recipient']
        to_address = data[transaction_hash]['outputs'][0]['recipient']
        transactions.append((transaction_hash, from_address, to_address, user_id))
    return transactions


def retrieve_latest_transaction_hashes(address):
    url_1 = f'https://api.blockchair.com/bitcoin/dashboards/address/{address}'
    response = requests.get(url_1)
    data = response.json()['data']
    transaction_hashes = data[address]['transactions']
    return transaction_hashes


def get_new_transaction_hashes(transactions, transaction_hashes):
    """
    Get new transaction hashes by comparing current
    :param transactions:
    :param transaction_hashes:
    :return:
    """
    current_transaction_hashes = set()
    for transaction in transactions:
        current_transaction_hashes.add(transaction[1])
    # print(f'current_transaction_hashes is {current_transaction_hashes}')
    new_transaction_hashes = []
    for transaction_hash in transaction_hashes:
        if transaction_hash not in current_transaction_hashes:
            new_transaction_hashes.append(transaction_hash)
    return new_transaction_hashes


def update_balance(conn, address):
    """
    Update balance for given address
    :param conn:
    :param address:
    :return:
    """
    sql = ''' UPDATE addresses
                  SET address = ? ,
                      user_id = ? ,
                      balance = ? ,
                      last_synced_time = ?
                  WHERE id = ?'''
    cur = conn.cursor()
    cur.execute(sql, address)
    conn.commit()


def add_transactions(conn, transactions):
    """
    Add transactions to transactions table
    :param conn:
    :param transactions:
    :return:
    """
    for transaction in transactions:
        create_transaction(conn, transaction)


def get_all_records(conn):
    """
    Get all records from table
    :param conn: the Connection object
    :param table_name:
    :return:
    """
    cur = conn.cursor()
    cur.execute("SELECT * FROM users")

    users = cur.fetchall()
    cur.execute("SELECT * FROM addresses")

    addresses = cur.fetchall()
    cur.execute("SELECT * FROM transactions")

    transactions = cur.fetchall()
    return users, addresses, transactions


def main():
    conn = create_connection('./test.db')

    # create users, addresses, transactions tables
    create_user_address_transaction_tables(conn)

    # insert user liangzhang
    user_1 = ('liangzhang', 'liang', 'zhang', 'zhangliang****@gmail.com', '206000001')
    user_1_id = create_user(conn, user_1)

    address_1 = '3E8ociqZa9mZUSwGdSmAEMAoAxBK3FNDcd'
    address_2 = 'bc1qzqh7y60gj525577m87l2svfv3rp0akcjy7k0zs'
    transaction_hash = '3b88828b1afee7314ac3dce908f9e061338f03051aa3d422bffad27d43a34b10'

    # REQUIREMENT 1: Add bitcoin address, insert address_record_1 for user liangzhang
    address_record_1 = (address_1, user_1_id, '2006904', datetime.now())
    address_record_1_id = create_address(conn, address_record_1)

    # insert transaction_1 with transaction_hash_1 for user liangzhang
    transaction_1 = (transaction_hash, address_1, address_2, user_1_id)
    create_transaction(conn, transaction_1)

    # REQUIREMENT 2: View the current balance and historical transactions for each bitcoin address
    current_balance, current_transactions = get_current_balance_and_historical_transactions(conn, address_1)

    # REQUIREMENT 3: Synchronize data to retrieve the latest balances and transactions on each bitcoin address
    latest_balance = retrieve_latest_balance(address_1)
    if latest_balance != current_balance:
        address_record_1_with_updated_balance = (address_1, user_1_id, latest_balance, datetime.now(), address_record_1_id)
        update_balance(conn, address_record_1_with_updated_balance)
    latest_transaction_hashes = retrieve_latest_transaction_hashes(address_1)
    new_transaction_hashes = get_new_transaction_hashes(current_transactions, latest_transaction_hashes)
    new_transactions = retrieve_latest_transactions_details(new_transaction_hashes[95:], user_1_id)
    if new_transactions:
        add_transactions(conn, new_transactions)

    users, addresses, transactions = get_all_records(conn)

    expected_users = [(1, 'liangzhang', 'liang', 'zhang', 'zhangliang****@gmail.com', '206000001')]
    expected_addresses = [(1, '3E8ociqZa9mZUSwGdSmAEMAoAxBK3FNDcd', '1', '2012839', '2021-10-24 17:06:28.854515')]
    expected_transactions = [(1, '3b88828b1afee7314ac3dce908f9e061338f03051aa3d422bffad27d43a34b10', '3E8ociqZa9mZUSwGdSmAEMAoAxBK3FNDcd', 'bc1qzqh7y60gj525577m87l2svfv3rp0akcjy7k0zs', '1'), (2, 'ecaa28fca681c1b015f6261405815846059bac44ad574f71d7a9c128a19c91c4', '3FaL7eXpRSHBt8YeyVR7nfuRXydkv8ch21', '3E8ociqZa9mZUSwGdSmAEMAoAxBK3FNDcd', '1'), (3, '6ae8ced86239319f8b7eba2b4842589506265033d8325183cfe78becaa85a615', 'bc1qs7qpqjxy9ahpr3ddf9rhhqusjjd3vkc5fr7zh7', '3LnV7DjSaB5qZY1n7NkwK5HABEFNrznc9y', '1'), (4, '29f9f0930e93554577e79842acc335b83972fa6e5e93d2b62318b74eff582604', 'bc1q3jzpzukg63u0mq3ualdfmv5f6laq7ts737eets', 'bc1q6af60xnjzwukts6z4u9235jgjp4ced2zlwsan7', '1'), (5, '246aef6f4f8a8a9325b902180c47135df04a67cbaceb0065b0ede57654301c70', 'bc1qfmvpylmay5gm2mznhezp8kp2v9asah0utnuyuj', '3E8ociqZa9mZUSwGdSmAEMAoAxBK3FNDcd', '1')]
    print(users == expected_users)
    print(addresses == expected_addresses)
    print(transactions == expected_transactions)


if __name__ == '__main__':
    main()