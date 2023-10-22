import json
import time

from logging.config import dictConfig
from flask import Flask, jsonify, request
from flask.globals import app_ctx, current_app
from flask_swagger_ui import get_swaggerui_blueprint

from common.decorators import add_address_to_response
from config.config import API_HOST, API_PORT, NETWORK
from services.account import AccountService
from services.distribution import DistributionService
from services.parameters import ParametersService
from services.proposal import ProposalService
from services.statistics import StatisticsService
from services.validator import ValidatorService


dictConfig({
    'version': 1,
    'formatters': {'default': {
        'format': '[%(asctime)s] %(levelname)s in %(module)s: %(message)s',
    }},
    'handlers': {'wsgi': {
        'class': 'logging.StreamHandler',
        'stream': 'ext://flask.logging.wsgi_errors_stream',
        'formatter': 'default'
    }},
    'root': {
        'level': 'INFO',
        'handlers': ['wsgi']
    }
})

app = Flask(__name__)


@app.route('/swagger-ui')
def doc(): return open('./config/swagger.json').read()


SWAGGER_URL = '/swagger-ui'  # URL for exposing Swagger UI (without trailing '/')
API_URL = '/swagger-ui'  # Our API url (can of course be a local resource)


swaggerui_blueprint = get_swaggerui_blueprint(
    SWAGGER_URL,  # Swagger UI static files will be mapped to '{SWAGGER_URL}/dist/'
    API_URL,
    config={  # Swagger UI config overrides
        'app_name': "spacebox_api"
    }
)

# API using clickhouse
# @app.route('/account/account_balance/<address>')
# @add_address_to_response
# def account_balance(address):
#     account_service = AccountService()
#     return jsonify(account_service.get_account_balance(address))


# API using LCD API
@app.route('/account/account_balance/<address>')
@add_address_to_response
def account_balance(address):
    account_service = AccountService()
    return jsonify(account_service.get_account_balance_2(address))


@app.route('/account/validators/<address>')
@add_address_to_response
def account_validators(address):
    account_service = AccountService()
    return jsonify({'validators': account_service.get_validators(address)})


@app.route('/account/votes/<address>')
@add_address_to_response
def account_votes(address):
    proposal_id = request.args.get('proposal_id', None)
    account_service = AccountService()
    return jsonify({'votes': account_service.get_votes(address, proposal_id)})


@app.route('/account/account_info/<address>')
@add_address_to_response
def account_info(address):
    account_service = AccountService()
    return jsonify(account_service.get_account_info(address))


@app.route('/gov/proposal/<id>')
def proposal(id):
    proposal_service = ProposalService()
    return jsonify(proposal_service.get_proposal(id))


@app.route('/gov/proposals')
def proposals():
    proposal_service = ProposalService()
    limit = request.args.get('limit')
    offset = request.args.get('offset')
    return jsonify({'proposals': proposal_service.get_proposals(limit, offset, request.args)})


@app.route('/gov/votes')
def votes():
    proposal_service = ProposalService()
    limit = request.args.get('limit')
    offset = request.args.get('offset')
    return jsonify({'votes': proposal_service.get_votes(limit, offset)})


@app.route('/gov/votes/<id>')
def vote(id):
    proposal_service = ProposalService()
    return jsonify(proposal_service.get_vote(id))


@app.route('/gov/votes/<id>/validators-info')
def vote_based_on_validators(id):
    proposal_service = ProposalService()
    validator_option = request.args.get('validator_option')
    return jsonify({'delegators': proposal_service.get_delegators_votes_info_for_proposal(id, validator_option)})


@app.route('/gov/votes/<id>/validators-info/<validator_address>')
def votes_of_specific_validator(id, validator_address):
    proposal_service = ProposalService()
    return jsonify(proposal_service.get_validator_delegators_votes_info_for_proposal(id, validator_address))


@app.route('/statistics/validators')
def validators():
    validator_service = ValidatorService()
    limit = request.args.get('limit')
    offset = request.args.get('offset')
    return jsonify({'validators': validator_service.get_validators(limit, offset)})


@app.route('/statistics/validators/group_map')
def validators_group_map():
    validator_service = ValidatorService()
    return jsonify({'validators': validator_service.get_validators_group_map()})


@app.route('/statistics/validators/<operator_address>')
def validator_by_operator_address(operator_address):
    validator_service = ValidatorService()
    return jsonify(validator_service.get_validator_by_operator_address(operator_address))


@app.route('/statistics/validators/<operator_address>/commissions')
def validator_by_operator_address_commissions(operator_address):
    from_date = request.args.get('from_date')
    to_date = request.args.get('to_date')
    detailing = request.args.get('detailing')
    result = ValidatorService().get_validator_commissions(from_date, to_date, detailing, operator_address)
    return jsonify({'data': result, 'name': 'validator_commissions'})


@app.route('/statistics/validators/<operator_address>/rewards')
def validator_by_operator_address_rewards(operator_address):
    from_date = request.args.get('from_date')
    to_date = request.args.get('to_date')
    detailing = request.args.get('detailing')
    result = ValidatorService().get_validator_rewards(from_date, to_date, detailing, operator_address)
    return jsonify({'data': result, 'name': 'validator_rewards'})


@app.route('/statistics/validators/<operator_address>/voting_power')
def validator_voting_power(operator_address):
    from_date = request.args.get('from_date')
    to_date = request.args.get('to_date')
    detailing = request.args.get('detailing')
    result = ValidatorService().get_validator_voting_power(from_date, to_date, detailing, operator_address)
    return jsonify({'data': result, 'name': 'validator_voting_power'})



@app.route('/statistics/validators/<operator_address>/uptime_stat')
def validator_by_operator_address_uptime_stat(operator_address):
    from_date = request.args.get('from_date')
    to_date = request.args.get('to_date')
    detailing = request.args.get('detailing')
    result = ValidatorService().get_validator_uptime_stat(from_date, to_date, detailing, operator_address)
    return jsonify({'data': result, 'name': 'validator_uptime_stat'})


@app.route('/statistics/fees_paid')
def fees_paid():
    from_date = request.args.get('from_date')
    to_date = request.args.get('to_date')
    detailing = request.args.get('detailing')
    result = StatisticsService().get_fees_paid(from_date, to_date, detailing)
    return jsonify({'data': result, 'name': 'fees_paid'})


@app.route('/statistics/fees_paid/actual')
def fees_paid_actual():
    result = StatisticsService().get_fees_paid_actual()
    return jsonify({'data': result, 'name': 'fees_paid_actual'})


@app.route('/validators/<validator_address>')
def validator(validator_address):
    validator_service = ValidatorService()
    return jsonify(validator_service.get_validator_info(validator_address))


@app.route('/distribution/staking_pool')
def staking_pool():
    distribution_service = DistributionService()
    return jsonify(distribution_service.get_staking_pool())


@app.route('/statistics/active_proposals')
def active_proposals():
    statistics_service = StatisticsService()
    return jsonify({'data': statistics_service.get_active_proposals_statistics(), 'name': 'active_proposals'})


@app.route('/statistics/pending_proposals')
def pending_proposals():
    statistics_service = StatisticsService()
    return jsonify({'data': statistics_service.get_pending_proposals_statistics(), 'name': 'pending_proposals'})

@app.route('/statistics/last_block_height')
def last_block_height():
    statistics_service = StatisticsService()
    return jsonify({'data': statistics_service.get_last_block_height(), 'name': 'last_block_height'})


@app.route('/statistics/blocks_time')
def blocks_time():
    statistics_service = StatisticsService()
    return jsonify({'data': statistics_service.get_blocks_time(), 'name': 'blocks_time'})

@app.route('/statistics/blocks')
def blocks():
    from_date = request.args.get('from_date')
    to_date = request.args.get('to_date')
    detailing = request.args.get('detailing')
    statistics_service = StatisticsService()
    return jsonify({'data': statistics_service.get_blocks(from_date, to_date, detailing), 'name': 'blocks'})


@app.route('/statistics/transactions_per_block')
def transactions_per_block():
    statistics_service = StatisticsService()
    limit = request.args.get('limit')
    offset = request.args.get('offset')
    return jsonify({'data': statistics_service.get_transactions_per_block(limit, offset), 'name': 'transactions_per_block'})


@app.route('/statistics/active_validators')
def active_validators():
    statistics_service = StatisticsService()
    return jsonify({'data': statistics_service.get_active_validators(), 'name': 'active_validators'})


@app.route('/statistics/unbound_period')
def unbound_period():
    statistics_service = StatisticsService()
    return jsonify({'data': statistics_service.get_unbound_period(), 'name': 'unbound_period'})


@app.route('/statistics/market_cap')
def market_cap():
    statistics_service = StatisticsService()
    return jsonify({'data': statistics_service.get_market_cap(), 'name': 'market_cap'})


@app.route('/statistics/token_prices')
def token_prices():
    statistics_service = StatisticsService()
    return jsonify({'data': statistics_service.get_token_prices(), 'name': 'token_prices'})


@app.route('/statistics/total_supply')
def total_supply():
    from_date = request.args.get('from_date')
    to_date = request.args.get('to_date')
    detailing = request.args.get('detailing')
    statistics_service = StatisticsService()
    return jsonify({'data': statistics_service.get_total_supply_by_days(from_date, to_date, detailing), 'name': 'total_supply'})


@app.route('/statistics/total_supply/actual')
def total_supply_actual():
    statistics_service = StatisticsService()
    return jsonify({'data': statistics_service.get_total_supply_actual(), 'name': 'total_supply_actual'})


@app.route('/statistics/bonded_tokens')
def bonded_tokens():
    from_date = request.args.get('from_date')
    to_date = request.args.get('to_date')
    detailing = request.args.get('detailing')
    statistics_service = StatisticsService()
    return jsonify({'data': statistics_service.get_bonded_tokens_by_days(from_date, to_date, detailing), 'name': 'bonded_atom'})


@app.route('/statistics/unbonded_tokens')
def unbonded_tokens():
    from_date = request.args.get('from_date')
    to_date = request.args.get('to_date')
    detailing = request.args.get('detailing')
    statistics_service = StatisticsService()
    return jsonify({'data': statistics_service.get_unbonded_tokens_by_days(from_date, to_date, detailing), 'name': 'unbonded_atom'})


@app.route('/statistics/unbonded_tokens/actual')
def unbonded_tokens_actual():
    statistics_service = StatisticsService()
    return jsonify({'data': statistics_service.get_unbonded_tokens_actual(), 'name': 'unbonded_atom_actual'})


@app.route('/statistics/bonded_tokens/actual')
def bonded_tokens_actual():
    statistics_service = StatisticsService()
    return jsonify({'data': statistics_service.get_bonded_tokens_actual(), 'name': 'bonded_atom_actual'})


@app.route('/statistics/circulating_supply')
def circulating_supply():
    from_date = request.args.get('from_date')
    to_date = request.args.get('to_date')
    detailing = request.args.get('detailing')
    statistics_service = StatisticsService()
    return jsonify({'data': statistics_service.get_circulating_supply_by_days(from_date, to_date, detailing), 'name': 'circulating_supply'})


@app.route('/statistics/circulating_supply/actual')
def circulating_supply_actual():
    statistics_service = StatisticsService()
    return jsonify({'data': statistics_service.get_circulating_supply_actual(), 'name': 'circulating_supply_actual'})


@app.route('/statistics/bonded_ratio')
def bonded_ratio():
    from_date = request.args.get('from_date')
    to_date = request.args.get('to_date')
    detailing = request.args.get('detailing')
    statistics_service = StatisticsService()
    return jsonify({'data': statistics_service.get_bonded_ratio_by_days(from_date, to_date, detailing), 'name': 'bonded_ratio'})


@app.route('/statistics/bonded_ratio/actual')
def bonded_ratio_actual():
    statistics_service = StatisticsService()
    return jsonify({'data': statistics_service.get_bonded_ratio_actual(), 'name': 'bonded_ratio_actual'})


@app.route('/statistics/community_pool')
def community_pool():
    from_date = request.args.get('from_date')
    to_date = request.args.get('to_date')
    detailing = request.args.get('detailing')
    statistics_service = StatisticsService()
    return jsonify({'data': statistics_service.get_community_pool_by_days(from_date, to_date, detailing), 'name': 'community_pool'})


@app.route('/statistics/community_pool/actual')
def community_pool_actual():
    statistics_service = StatisticsService()
    return jsonify({'data': statistics_service.get_community_pool_actual(), 'name': 'community_pool_actual'})


@app.route('/statistics/inflation')
def inflation():
    from_date = request.args.get('from_date')
    to_date = request.args.get('to_date')
    detailing = request.args.get('detailing')
    statistics_service = StatisticsService()
    return jsonify({'data': statistics_service.get_inflation_by_days(from_date, to_date, detailing), 'name': 'inflation'})


@app.route('/statistics/inflation/actual')
def inflation_actual():
    statistics_service = StatisticsService()
    return jsonify({'data': statistics_service.get_inflation_actual(), 'name': 'inflation_actual'})


@app.route('/statistics/apr')
def apr():
    from_date = request.args.get('from_date')
    to_date = request.args.get('to_date')
    detailing = request.args.get('detailing')
    statistics_service = StatisticsService()
    return jsonify({'data': statistics_service.get_apr_by_days(from_date, to_date, detailing), 'name': 'apr'})


@app.route('/statistics/restake_execution_count')
def restake_execution_count():
    from_date = request.args.get('from_date')
    to_date = request.args.get('to_date')
    detailing = request.args.get('detailing')
    statistics_service = StatisticsService()
    return jsonify({'data': statistics_service.get_restake_execution_count(from_date, to_date, detailing), 'name': 'restake_execution_count'})


@app.route('/statistics/restake_execution_count/actual')
def restake_execution_count_actual():
    statistics_service = StatisticsService()
    return jsonify({'data': statistics_service.get_restake_execution_count_actual(), 'name': 'restake_execution_count_actual'})


@app.route('/statistics/apr/actual')
def apr_actual():
    statistics_service = StatisticsService()
    return jsonify({'data': statistics_service.get_apr_actual(), 'name': 'apr_actual'})


@app.route('/statistics/apy')
def apy():
    from_date = request.args.get('from_date')
    to_date = request.args.get('to_date')
    detailing = request.args.get('detailing')
    statistics_service = StatisticsService()
    return jsonify({'data': statistics_service.get_apy_by_days(from_date, to_date, detailing), 'name': 'apy'})


@app.route('/statistics/apy/actual')
def apy_actual():
    statistics_service = StatisticsService()
    return jsonify({'data': statistics_service.get_apy_actual(), 'name': 'apy_actual'})


@app.route('/statistics/total_accounts/actual')
def total_accounts_actual():
    statistics_service = StatisticsService()
    return jsonify({'data': statistics_service.get_total_accounts_actual(), 'name': 'total_accounts_actual'})

# TODO: THINK ABOUT VIEW
@app.route('/statistics/total_accounts')
def total_accounts():
    from_date = request.args.get('from_date')
    to_date = request.args.get('to_date')
    detailing = request.args.get('detailing')
    statistics_service = StatisticsService()
    return jsonify({'data': statistics_service.get_total_accounts(from_date, to_date, detailing), 'name': 'total_accounts'})


@app.route('/statistics/popular_transactions')
def popular_transactions():
    statistics_service = StatisticsService()
    return jsonify({'data': statistics_service.get_popular_transactions(), 'name': 'popular_transactions'})


@app.route('/statistics/staked_statistics')
def staked_statistics():
    statistics_service = StatisticsService()
    return jsonify({'data': statistics_service.get_staked_statistics(), 'name': 'staked_statistics'})


@app.route('/statistics/wealth_distribution')
def wealth_distribution():
    statistics_service = StatisticsService()
    return jsonify({'data': statistics_service.get_wealth_distribution(), 'name': 'wealth_distribution'})


@app.route('/statistics/inactive_accounts')
def inactive_accounts():
    statistics_service = StatisticsService()
    return jsonify({'data': statistics_service.get_inactive_accounts(), 'name': 'inactive_accounts'})


@app.route('/statistics/new_accounts')
def new_accounts():
    from_date = request.args.get('from_date')
    to_date = request.args.get('to_date')
    detailing = request.args.get('detailing')
    statistics_service = StatisticsService()
    return jsonify({'data': statistics_service.get_new_accounts(from_date, to_date, detailing), 'name': 'new_accounts'})


@app.route('/statistics/new_accounts/actual')
def new_accounts_actual():
    statistics_service = StatisticsService()
    return jsonify({'data': statistics_service.get_new_accounts_actual(), 'name': 'new_accounts_actual'})


@app.route('/statistics/gas_paid')
def gas_paid():
    from_date = request.args.get('from_date')
    to_date = request.args.get('to_date')
    detailing = request.args.get('detailing')
    statistics_service = StatisticsService()
    return jsonify({'data': statistics_service.get_gas_paid(from_date, to_date, detailing), 'name': 'gas_paid'})


@app.route('/statistics/gas_paid/actual')
def gas_paid_actual():
    statistics_service = StatisticsService()
    return jsonify({'data': statistics_service.get_gas_paid_actual(), 'name': 'gas_paid_actual'})


@app.route('/statistics/transactions')
def transactions():
    from_date = request.args.get('from_date')
    to_date = request.args.get('to_date')
    detailing = request.args.get('detailing')
    statistics_service = StatisticsService()
    return jsonify({'data': statistics_service.get_transactions(from_date, to_date, detailing), 'name': 'transactions'})


@app.route('/statistics/transactions/actual')
def transactions_actual():
    statistics_service = StatisticsService()
    return jsonify({'data': statistics_service.get_transactions_actual(), 'name': 'transactions_actual'})


@app.route('/statistics/redelegation_message')
def redelegation_message():
    from_date = request.args.get('from_date')
    to_date = request.args.get('to_date')
    detailing = request.args.get('detailing')
    statistics_service = StatisticsService()
    return jsonify({'data': statistics_service.get_redelegation_message(from_date, to_date, detailing), 'name': 'redelegation_message'})


@app.route('/statistics/unbonding_message')
def unbonding_message():
    from_date = request.args.get('from_date')
    to_date = request.args.get('to_date')
    detailing = request.args.get('detailing')
    statistics_service = StatisticsService()
    return jsonify({'data': statistics_service.get_unbonding_message(from_date, to_date, detailing), 'name': 'unbonding_message'})


@app.route('/statistics/delegation_message')
def delegation_message():
    from_date = request.args.get('from_date')
    to_date = request.args.get('to_date')
    detailing = request.args.get('detailing')
    statistics_service = StatisticsService()
    return jsonify({'data': statistics_service.get_delegation_message(from_date, to_date, detailing), 'name': 'delegation_message'})


@app.route('/statistics/active_accounts')
def active_accounts():
    from_date = request.args.get('from_date')
    to_date = request.args.get('to_date')
    detailing = request.args.get('detailing')
    statistics_service = StatisticsService()
    return jsonify({'data': statistics_service.get_active_accounts(from_date, to_date, detailing), 'name': 'active_accounts'})


@app.route('/statistics/active_accounts/actual')
def active_accounts_actual():
    statistics_service = StatisticsService()
    return jsonify({'data': statistics_service.get_active_accounts_actual(), 'name': 'active_accounts_actual'})


@app.route('/parameters/staking')
def staking():
    parameters_service = ParametersService()
    return jsonify(parameters_service.get_staking_params())


@app.route('/parameters/mint')
def mint():
    parameters_service = ParametersService()
    return jsonify(parameters_service.get_mint_params())


@app.route('/parameters/distribution')
def distribution():
    parameters_service = ParametersService()
    return jsonify(parameters_service.get_distribution_params())


@app.route('/parameters/slash')
def slash():
    parameters_service = ParametersService()
    return jsonify(parameters_service.get_slash_params())


@app.route('/parameters/gov')
def gov():
    parameters_service = ParametersService()
    return jsonify(parameters_service.get_gov_parameters())


@app.route('/statistics/restake_token_amount')
def restake_token_amount():
    from_date = request.args.get('from_date')
    to_date = request.args.get('to_date')
    detailing = request.args.get('detailing')
    statistics_service = StatisticsService()
    return jsonify({'data': statistics_service.get_restake_token_amount(from_date, to_date, detailing), 'name': 'restake_token_amount'})


@app.route('/statistics/restake_token_amount/actual')
def restake_token_amount_actual():
    statistics_service = StatisticsService()
    return jsonify({'data': statistics_service.get_restake_token_amount_actual(), 'name': 'restake_token_amount_actual'})

@app.route('/statistics/whale_transactions')
def whale_transactions():
    statistics_service = StatisticsService()
    limit = request.args.get('limit')
    offset = request.args.get('offset')
    return jsonify({'data': statistics_service.get_whale_transactions(limit, offset), 'name': 'whale_transactions'})


@app.route('/statistics/staked_amount', methods=['POST'])
def staked_amount():
    request_data = request.get_json()
    user_addresses = request_data.get('addresses', [])
    statistics_service = StatisticsService()
    return jsonify({'data': statistics_service.get_user_bronbro_staking(user_addresses), 'name': 'staked_amount'})


@app.before_request
def logging_before():
    # Store the start time for the request
    app_ctx.start_time = time.perf_counter()


@app.after_request
def add_network_and_response_time_to_response(response):
    total_time = time.perf_counter() - app_ctx.start_time
    time_in_ms = int(total_time * 1000)
    # Log the time taken for the endpoint
    app.logger.info(f'Response time: {time_in_ms}, path: {request.path}')
    data = response.json
    if data:
        data['network'] = NETWORK
        data['response_time'] = time_in_ms
        response.data = json.dumps(data)
    return response


if __name__ == '__main__':
    app.register_blueprint(swaggerui_blueprint)
    app.run(host=API_HOST, port=API_PORT)
