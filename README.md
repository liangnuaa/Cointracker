# Cointracker
assignment for cointracker

Simply run:
```python3 assignment.py```
It will check the all records in users, addresses and transactions table. Three test case should all pases.

First, the prototype first creates users, addresses, transactions tables.

Second it inserts one user record liangzhang, one address_record_1 for user liangzhang and one transaction record for user liang zhang and address address_record_1

```user_1 = ('liangzhang', 'liang', 'zhang', 'zhangliang****@gmail.com', '206000001')```

```address_record_1 = (address_1, user_1_id, '2006904', datetime.now())```

```transaction_1 = (transaction_hash, address_1, address_2, user_1_id)```

Third, it views the current balance and historical transactions for address_record_1

```current_balance, current_transactions = get_current_balance_and_historical_transactions(conn, address_1)```


Fourth, it synchronize data to retrieve the latest balances and transactions for address_record_1 by calling blockchair api.
Update balance for address_record_1 if latest is different than the current one. If there are new transactions, also insert those transactions to transactions table. (due to too many transaction records for given bitcoin address, and some of transaction detail can't be found by calling https://api.blockchair.com/bitcoin/dashboards/transaction/{transaction_hash}, so just choose the new records starting from index 96 to test the functionality)

```latest_balance = retrieve_latest_balance(address_1)
    if latest_balance != current_balance:
        address_record_1_with_updated_balance = (address_1, user_1_id, latest_balance, datetime.now(), address_record_1_id)
        update_balance(conn, address_record_1_with_updated_balance)
    latest_transaction_hashes = retrieve_latest_transaction_hashes(address_1)
    new_transaction_hashes = get_new_transaction_hashes(current_transactions, latest_transaction_hashes)
    new_transactions = retrieve_latest_transactions_details(new_transaction_hashes[95:], user_1_id)
    if new_transactions:
        add_transactions(conn, new_transactions)
```

Finally, verify all three table records

```
users, addresses, transactions = get_all_records(conn)

    expected_users = [(1, 'liangzhang', 'liang', 'zhang', 'zhangliang****@gmail.com', '206000001')]
    expected_addresses = [(1, '3E8ociqZa9mZUSwGdSmAEMAoAxBK3FNDcd', '1', '2012839', '2021-10-24 17:06:28.854515')]
    expected_transactions = [(1, '3b88828b1afee7314ac3dce908f9e061338f03051aa3d422bffad27d43a34b10', '3E8ociqZa9mZUSwGdSmAEMAoAxBK3FNDcd', 'bc1qzqh7y60gj525577m87l2svfv3rp0akcjy7k0zs', '1'), (2, 'ecaa28fca681c1b015f6261405815846059bac44ad574f71d7a9c128a19c91c4', '3FaL7eXpRSHBt8YeyVR7nfuRXydkv8ch21', '3E8ociqZa9mZUSwGdSmAEMAoAxBK3FNDcd', '1'), (3, '6ae8ced86239319f8b7eba2b4842589506265033d8325183cfe78becaa85a615', 'bc1qs7qpqjxy9ahpr3ddf9rhhqusjjd3vkc5fr7zh7', '3LnV7DjSaB5qZY1n7NkwK5HABEFNrznc9y', '1'), (4, '29f9f0930e93554577e79842acc335b83972fa6e5e93d2b62318b74eff582604', 'bc1q3jzpzukg63u0mq3ualdfmv5f6laq7ts737eets', 'bc1q6af60xnjzwukts6z4u9235jgjp4ced2zlwsan7', '1'), (5, '246aef6f4f8a8a9325b902180c47135df04a67cbaceb0065b0ede57654301c70', 'bc1qfmvpylmay5gm2mznhezp8kp2v9asah0utnuyuj', '3E8ociqZa9mZUSwGdSmAEMAoAxBK3FNDcd', '1')]
    print(users == expected_users)
    print(addresses[0][1] == expected_addresses[0][1] and addresses[0][2] == expected_addresses[0][2] and addresses[0][3] == expected_addresses[0][3])
    print(transactions == expected_transactions)
```