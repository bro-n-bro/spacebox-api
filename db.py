from config.config import CLICKHOUSE_HOST, CLICKHOUSE_PASSWORD, CLICKHOUSE_PORT, CLICKHOUSE_USERNAME, LCD_API,\
                            STAKED_DENOM
import clickhouse_connect
import requests


def get_client():
    client = clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USERNAME,
        password=CLICKHOUSE_PASSWORD
    )
    return client


def get_account_liquid_balance(address):
    c = get_client()
    result = c.query(f'''
        SELECT address, coins FROM spacebox.account_balance FINAL
        WHERE address = '{address}'
    ''')
    res = result.result_rows[0]
    c.close()
    res = dict(zip(res[1]['denom'], res[1]['amount']))
    return {"liquid": res}


def get_account_staked_balance(address):
    c = get_client()
    result = c.query(f'''
        SELECT delegator_address, coin FROM spacebox.delegation FINAL
        WHERE delegator_address = '{address}'
    ''')
    res = result.result_rows
    c.close()
    amount = sum([b[1]['amount'] for b in res])
    return {"staked": {STAKED_DENOM: amount}}


def get_account_unbonding_balance(address):
    c = get_client()
    result = c.query(f'''
        SELECT delegator_address, coin FROM spacebox.unbonding_delegation FINAL
        WHERE delegator_address = '{address}'
        AND completion_timestamp > now()
    ''')
    res = result.result_rows
    c.close()
    amount = sum([b[1]['amount'] for b in res])
    return {"unbonding": {STAKED_DENOM: amount}}


def get_account_commission_balance(address):
    res = requests.get(LCD_API + f'/cosmos/distribution/v1beta1/delegators/{address}/rewards').json()
    rewards = res['total']
    rewards = [int(float(r['amount'])) for r in rewards if r['denom'] == STAKED_DENOM][0]
    return {"rewards": {STAKED_DENOM: rewards}}


def get_account_balance(address):
    liquid = get_account_liquid_balance(address)
    liquid.update(get_account_staked_balance(address))
    liquid.update(get_account_unbonding_balance(address))
    liquid.update(get_account_commission_balance(address))
    liquid.update({"outside": {STAKED_DENOM: 0}})
    return liquid


def get_account_info(address):
    return {
        "apr": 0.12,
        "voting_power": 0.000000012,
        "rpde": 124000,
        "staked": {STAKED_DENOM: 1240234},
        "annual_provision": 1234
    }


def get_validators(address):
    c = get_client()
    result = c.query(f'''
        SELECT 
            operator_address,
            moniker,
            delegator_address,
            coin,
            identity,
            avatar_url,
            website,
            security_contact,
            details
        FROM (
            SELECT * FROM spacebox.delegation FINAL
            WHERE delegator_address = '{address}'
        ) AS _t
        LEFT JOIN (
            SELECT * FROM spacebox.validator_description FINAL
        ) AS t ON _t.operator_address = t.operator_address 
    ''')
    res = result.result_rows
    columns = result.column_names
    c.close()
    validators = [dict(zip(columns, r)) for r in res if r[3]['amount'] > 0]
    return validators


def get_proposals(limit=10, offset=0):
    c = get_client()
    result = c.query(f'''
        SELECT * FROM spacebox.proposal FINAL
        WHERE proposer_address NOT IN [
            'cosmos13wmuxhswmr9tfv46n7g3qmjsavv6qe2y5ntq8f', 
            'cosmos1vgyvt6xml92vl9l04qj3vqffqyz4j4esyzdxny',
            'cosmos1s4j5gt0pjveuezsh8eutzzvv0smgqhc9f0jtt5',
            'cosmos1sn2w42aaq8cu3hgjgv0afv3tmhnueyq3v5wqsh',
            'cosmos1wq4kp9getwt7vvrxn5w8jh8fwmnsquy0c7rvr4',
            'cosmos1e7zatfppdud3lf5xp32xs7d5ulz7qsl9xuy76y',
            'cosmos1s3g8juhskgnxjd5457vev20gm02ku2ruk9myz5',
            'cosmos1huzu0hfaps0skk2l7rzej788qj6sgepmrr466x',
            'cosmos1l4qqnt2ezpamfkh6ra3rvcsd8q25g4jkxlchvj',
            'cosmos1dsk8s6q9r5ad8x70jnyxs9tv4wxj6fat2js3yx',
            'cosmos17w6006zyc734gspv7meg3qqae8gxjahmgudhv7']
        ORDER BY id DESC 
        LIMIT {limit} OFFSET {offset}
    ''')
    res = result.result_rows
    columns = result.column_names
    c.close()
    proposals = [dict(zip(columns, r)) for r in res]
    return proposals