from flask import Blueprint, jsonify, render_template, make_response
from flask_login import login_required

external_log = Blueprint("external_log", __name__, url_prefix="/external_log", template_folder='templates')

@external_log.route('/live', methods=['GET'], endpoint='live-ALPHA000')
@login_required
def custom_api():
    return render_template('live.html')

@external_log.route('/put', defaults={'message': 'This is a custom Airflow external_log message!'}, methods=['GET'], endpoint='PUT-ALPHA000')
@external_log.route('/put/<message>', methods=['GET'])
@login_required
def custom_api(message: str):
    data = {'message': message}
    print(data)
    # web socker send message value to ui
    return make_response(jsonify(data), 200)

@login_required
@external_log.route('/test', methods=['GET'], endpoint='TEST-ALPHA000')
def html():
    return render_template('test.html', a=[1,2])
