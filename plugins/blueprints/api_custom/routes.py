from flask import Blueprint, jsonify, render_template

get_message = Blueprint("get_message", __name__, url_prefix="/get_message", template_folder='templates')

@get_message.route('/', defaults={'message': 'This is a custom Airflow endpoint!'}, methods=['GET', 'POST'])
@get_message.route('/<message>', methods=['GET', 'POST'])
def custom_api(message: str):
    # Implement your custom logic here
    data = {'message': message}
    return jsonify(data)

@get_message.route('/h')
def html():
    return render_template('test.html', a=[1,2])
