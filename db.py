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
    if res != {}:
        return {"liquid": res}
    else:
        return {"liquid": 0}


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
    try:
        res = requests.get(LCD_API + f'/cosmos/distribution/v1beta1/delegators/{address}/rewards').json()
        rewards = res['total']
        rewards = [int(float(r['amount'])) for r in rewards if r['denom'] == STAKED_DENOM][0]
        return {"rewards": {STAKED_DENOM: rewards}}
    except Exception as e:
        return {"rewards": {STAKED_DENOM: 0}}


def get_account_balance(address):
    liquid = get_account_liquid_balance(address)
    liquid.update(get_account_staked_balance(address))
    liquid.update(get_account_unbonding_balance(address))
    liquid.update(get_account_commission_balance(address))
    liquid.update({"outside": {STAKED_DENOM: 0}})
    return liquid


def get_annual_provision():
    try:
        res = requests.get(LCD_API + f'/cosmos/mint/v1beta1/annual_provisions').json()
        annual_provisions = int(float(res['annual_provisions']))
        return {"annual_provision": annual_provisions}
    except Exception as e:
        return {"annual_provision": 0}


def get_community_tax():
    c = get_client()
    result = c.query(f'''
        select * from spacebox.distribution_params final
        order by height desc
        limit 1
    ''')
    res = result.result_rows
    c.close()
    community_tax = res[0][1]['community_tax']
    return {"community_tax": community_tax}


def get_bonded_tokens_amount():
    c = get_client()
    result = c.query(f'''
        select * from spacebox.staking_pool final
        order by height desc
        limit 1
    ''')
    res = result.result_rows
    c.close()
    bonded_tokens = res[0][2]
    return {"bonded_tokens": bonded_tokens}


def get_account_info(address):
    validators = get_validators(address)
    delegations_sum = sum([x['coin']['amount'] for x in validators])
    annual_provision = get_annual_provision()['annual_provision']
    community_tax = get_community_tax()['community_tax']
    bonded_tokens_amount = get_bonded_tokens_amount()['bonded_tokens']
    apr = annual_provision * (1 - community_tax) / bonded_tokens_amount
    total_annual_provision = sum([x['coin']['amount'] * apr * (1 - x['commission']) for x in validators])
    return {
        "apr": apr,
        "voting_power": delegations_sum / bonded_tokens_amount,
        "rpde": total_annual_provision / 365.3,
        "staked": delegations_sum,
        "annual_provision": total_annual_provision
    }


def get_validators(address):
    c = get_client()
    result = c.query(f'''
        SELECT 
            t.operator_address as operator_address,
            moniker,
            delegator_address,
            coin,
            identity,
            avatar_url,
            website,
            security_contact,
            details,
            commission,
            max_change_rate,
            max_rate
        FROM (
            SELECT * FROM spacebox.delegation FINAL
            WHERE delegator_address = '{address}'
        ) AS _t
        LEFT JOIN (
            SELECT * FROM spacebox.validator_description FINAL
        ) AS t ON _t.operator_address = t.operator_address
        LEFT JOIN (
            SELECT * FROM spacebox.validator_commission FINAL
        ) AS c ON _t.operator_address = c.operator_address 
    ''')
    res = result.result_rows
    columns = result.column_names
    c.close()
    validators = [dict(zip(columns, r)) for r in res if r[3]['amount'] > 0]
    return validators


def get_proposals(limit=10, offset=0):
    c = get_client()
    result = c.query(f'''
        SELECT 
            id,
            title,
            description,
            content,
            proposal_route,
            proposal_type,
            submit_time,
            deposit_end_time,
            voting_start_time,
            voting_end_time,
            proposer_address,
            status,
            deposit,
            tally.yes,
            tally.abstain,
            tally.no,
            tally.no_with_veto,
            init_deposit,
            VOTE_OPTION_YES,
            VOTE_OPTION_NO,
            VOTE_OPTION_ABSTAIN,
            VOTE_OPTION_NO_WITH_VETO
        FROM spacebox.proposal FINAL
        LEFT JOIN
          ( SELECT proposal_id,
                  sum(tupleElement(coins,1)[1]) AS deposit
          FROM spacebox.proposal_deposit_message FINA
          GROUP BY proposal_id) AS deposit ON spacebox.proposal.id = deposit.proposal_id
        LEFT JOIN
          ( SELECT *
          FROM spacebox.proposal_tally_result FINAL) AS tally ON spacebox.proposal.id = tally.proposal_id
        LEFT JOIN (
            SELECT proposal_id, init_deposit FROM (
                SELECT proposal_id, tupleElement(coins,1)[1] AS init_deposit, rank() OVER(PARTITION BY proposal_id ORDER BY height ASC) RowNumber
                FROM spacebox.proposal_deposit_message FINAL
            ) AS init_deposit
            WHERE RowNumber = 1
        ) AS init_deposit ON spacebox.proposal.id = init_deposit.proposal_id
        LEFT JOIN (
            SELECT 
                yes.proposal_id as proposal_id,
                VOTE_OPTION_YES,
                VOTE_OPTION_NO,
                VOTE_OPTION_ABSTAIN,
                VOTE_OPTION_NO_WITH_VETO
            FROM (
                SELECT proposal_id, count(*) AS VOTE_OPTION_YES FROM spacebox.proposal_vote_message FINAL
                WHERE option = 'VOTE_OPTION_YES'
                GROUP BY proposal_id, option
            ) AS yes
            LEFT JOIN (
                SELECT proposal_id, count(*) AS VOTE_OPTION_NO FROM spacebox.proposal_vote_message FINAL
                WHERE option = 'VOTE_OPTION_NO'
                GROUP BY proposal_id, option
            ) AS no ON yes.proposal_id = no.proposal_id
            LEFT JOIN (
                SELECT proposal_id, count(*) AS VOTE_OPTION_ABSTAIN FROM spacebox.proposal_vote_message FINAL
                WHERE option = 'VOTE_OPTION_ABSTAIN'
                GROUP BY proposal_id, option
            ) AS abstain ON yes.proposal_id = abstain.proposal_id
            LEFT JOIN (
                SELECT proposal_id, count(*) AS VOTE_OPTION_NO_WITH_VETO FROM spacebox.proposal_vote_message FINAL
                WHERE option = 'VOTE_OPTION_NO_WITH_VETO'
                GROUP BY proposal_id, option
            ) AS nwv ON yes.proposal_id = nwv.proposal_id
        ) AS votes_count ON spacebox.proposal.id = votes_count.proposal_id
        WHERE deposit > 999999
        ORDER BY id DESC
        LIMIT {limit} OFFSET {offset}
    ''')
    res = result.result_rows
    columns = result.column_names
    c.close()
    proposals = [dict(zip(columns, r)) for r in res]
    return proposals


def get_proposal(id):
    c = get_client()
    result = c.query(f'''
        SELECT 
            id,
            title,
            description,
            content,
            proposal_route,
            proposal_type,
            submit_time,
            deposit_end_time,
            voting_start_time,
            voting_end_time,
            proposer_address,
            status,
            deposit,
            tally.yes,
            tally.abstain,
            tally.no,
            tally.no_with_veto,
            init_deposit,
            VOTE_OPTION_YES,
            VOTE_OPTION_NO,
            VOTE_OPTION_ABSTAIN,
            VOTE_OPTION_NO_WITH_VETO
        FROM spacebox.proposal FINAL
        LEFT JOIN
          ( SELECT proposal_id,
                  sum(tupleElement(coins,1)[1]) AS deposit
          FROM spacebox.proposal_deposit_message FINA
          GROUP BY proposal_id) AS deposit ON spacebox.proposal.id = deposit.proposal_id
        LEFT JOIN
          ( SELECT *
          FROM spacebox.proposal_tally_result FINAL) AS tally ON spacebox.proposal.id = tally.proposal_id
        LEFT JOIN (
            SELECT proposal_id, init_deposit FROM (
                SELECT proposal_id, tupleElement(coins,1)[1] AS init_deposit, rank() OVER(PARTITION BY proposal_id ORDER BY height ASC) RowNumber
                FROM spacebox.proposal_deposit_message FINAL
            ) AS init_deposit
            WHERE RowNumber = 1
        ) AS init_deposit ON spacebox.proposal.id = init_deposit.proposal_id
        LEFT JOIN (
            SELECT 
                yes.proposal_id as proposal_id,
                VOTE_OPTION_YES,
                VOTE_OPTION_NO,
                VOTE_OPTION_ABSTAIN,
                VOTE_OPTION_NO_WITH_VETO
            FROM (
                SELECT proposal_id, count(*) AS VOTE_OPTION_YES FROM spacebox.proposal_vote_message FINAL
                WHERE option = 'VOTE_OPTION_YES'
                GROUP BY proposal_id, option
            ) AS yes
            LEFT JOIN (
                SELECT proposal_id, count(*) AS VOTE_OPTION_NO FROM spacebox.proposal_vote_message FINAL
                WHERE option = 'VOTE_OPTION_NO'
                GROUP BY proposal_id, option
            ) AS no ON yes.proposal_id = no.proposal_id
            LEFT JOIN (
                SELECT proposal_id, count(*) AS VOTE_OPTION_ABSTAIN FROM spacebox.proposal_vote_message FINAL
                WHERE option = 'VOTE_OPTION_ABSTAIN'
                GROUP BY proposal_id, option
            ) AS abstain ON yes.proposal_id = abstain.proposal_id
            LEFT JOIN (
                SELECT proposal_id, count(*) AS VOTE_OPTION_NO_WITH_VETO FROM spacebox.proposal_vote_message FINAL
                WHERE option = 'VOTE_OPTION_NO_WITH_VETO'
                GROUP BY proposal_id, option
            ) AS nwv ON yes.proposal_id = nwv.proposal_id
        ) AS votes_count ON spacebox.proposal.id = votes_count.proposal_id
        WHERE id = {id}
        ORDER BY id DESC
    ''')
    res = result.result_rows
    columns = result.column_names
    c.close()
    proposal = [dict(zip(columns, r)) for r in res][0]
    return proposal