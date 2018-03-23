from flask import Flask
from flask import request
from flask import jsonify

from src.utils import Database, TaskAssignment

# DB constants
USER = 'federer'
PASSWORD = 'grandestslam'
DB = 'tennis'
HOST = 'localhost'
PORT = 5432


app = Flask(__name__)


@app.route('/next-task', methods=['GET'])
def tab_baseline():
    print(1111)
    job_id = request.args.get('jobID')
    worker_id = request.args.get('workerID')
    max_items = request.args.get('maxItems')

    # connect to database
    database = Database(USER, PASSWORD, DB, HOST, PORT)
    con, meta = database.connect()

    # task assignment baseline
    tab = TaskAssignment(con, job_id, worker_id, max_items)
    items, criteria = tab.get_tasks()

    response = {
            'items': items,
            'criteria': criteria
    }

    return jsonify(response)
