from flask import Blueprint, jsonify, render_template, make_response
from flask_login import login_required

get_message = Blueprint("get_message", __name__, url_prefix="/get_message", template_folder='templates')

@get_message.route('/', defaults={'message': 'This is a custom Airflow endpoint!'}, methods=['GET', 'POST'])
@get_message.route('/<message>', methods=['GET', 'POST'])
@login_required
def custom_api(message: str):
    data = {'message': message}
    print(data)
    return make_response(jsonify(data), 200)

@get_message.route('/h')
def html():
    return render_template('test.html', a=[1,2])
