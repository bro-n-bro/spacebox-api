import json

from flask import Flask, jsonify, request
from flask_swagger_ui import get_swaggerui_blueprint

from common.decorators import add_address_to_response
from config.config import API_HOST, API_PORT, NETWORK
from services.account import AccountService
from services.proposal import ProposalService

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

@app.route('/account/account_balance/<address>')
@add_address_to_response
def account_balance(address):
    account_service = AccountService()
    return jsonify(account_service.get_account_balance(address))


@app.route('/account/validators/<address>')
@add_address_to_response
def validators(address):
    account_service = AccountService()
    return jsonify({'validators': account_service.get_validators(address)})


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


@app.after_request
def add_network_to_response(response):
    data = response.json
    if data:
        data['network'] = NETWORK
        response.data = json.dumps(data)
    return response


if __name__ == '__main__':
    app.register_blueprint(swaggerui_blueprint)
    app.run(host=API_HOST, port=API_PORT)
