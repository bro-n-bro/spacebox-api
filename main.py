from flask import Flask, jsonify, request
from flask_swagger_ui import get_swaggerui_blueprint
from config.config import API_HOST, API_PORT
from db import get_account_balance, get_validators, get_account_info, get_proposals, get_proposal


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
def account_balance(address):
    return jsonify(get_account_balance(address))


@app.route('/account/validators/<address>')
def validators(address):
    return jsonify(get_validators(address))


@app.route('/account/account_info/<address>')
def account_info(address):
    return jsonify(get_account_info(address))


@app.route('/gov/proposal/<id>')
def proposal(id):
    return jsonify(get_proposal(id))


@app.route('/gov/proposals')
def proposals():
    limit = request.args.get('limit')
    offset = request.args.get('offset')
    if limit and offset:
        return jsonify(get_proposals(limit=limit, offset=offset))
    elif limit:
        return jsonify(get_proposals(limit=limit))
    elif offset:
        return jsonify(get_proposals(offset=offset))
    else:
        return jsonify(get_proposals())


if __name__ == '__main__':
    app.register_blueprint(swaggerui_blueprint)
    app.run(host=API_HOST, port=API_PORT)