from flask import Flask
from flask import request
from flask import jsonify

from src.task_assignment_box import Database, TaskAssignmentBaseline

# DB constants
USER = 'federer'
PASSWORD = 'grandestslam'
DB = 'tennis'
HOST = 'localhost'
PORT = 5432


app = Flask(__name__)


@app.route('/next-task', methods=['GET'])
def tab_baseline():
    job_id = request.args.get('jobID')
    worker_id = request.args.get('workerID')
    max_items = request.args.get('maxItems')

    # connect to database
    database = Database(USER, PASSWORD, DB, HOST, PORT)
    con, meta = database.connect()

    # task assignment baseline
    tab = TaskAssignmentBaseline(con, job_id, worker_id, max_items)
    items, criteria = tab.get_tasks()

    response = {
            'items': items,
            'criteria': criteria
    }

    return jsonify(response)
