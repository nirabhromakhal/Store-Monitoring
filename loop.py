# https://loopxyz.notion.site/Take-home-interview-Store-Monitoring-12664a3c7fdf472883a41457f0c9347d

import base64
import time
import threading
from flask import Flask, jsonify, Response, request
from flask_cors import CORS
from trigger_report import trigger_report_function, get_thread_from_report
import gdown
import os

# Download the required data files if necessary
dir_path = os.path.dirname(__file__)
if not os.path.exists(os.path.join(dir_path, "store_status.csv")):
    gdown.download(id="1UIx1hVJ7qt_6oQoGZgb8B3P2vd1FD025", output="store_status.csv", quiet=False)
if not os.path.exists(os.path.join(dir_path, "store_hours.csv")):
    gdown.download(id="1va1X3ydSh-0Rt1hsy2QSnHRA4w57PcXg", output="store_hours.csv", quiet=False)
if not os.path.exists(os.path.join(dir_path, "store_time_zones.csv")):
    gdown.download(id="101P9quxHoMZMZCVWQ5o-shonk2lgK1-o", output="store_time_zones.csv", quiet=False)

# initiate flask
app = Flask(__name__)

# allow cross-origin access to api
CORS(app)


@app.route("/trigger_report", methods=['GET'])
def trigger_report():
    # Generate report ID
    report_id = str(int(time.time()))

    # Start report generation in a different thread
    trigger_report_thread = threading.Thread(target=trigger_report_function, args=[report_id])
    trigger_report_thread.start()

    return report_id


@app.route("/get_report", methods=['POST'])
def get_report():
    body = request.get_json()
    report_id = body['report_id']

    response = {
        "status": None,
        "report.csv": None
    }

    if report_id in get_thread_from_report:
        response['status'] = "Running"
        return jsonify(response)
    elif not os.path.exists(os.path.join(dir_path, report_id + ".csv")):
        response['status'] = "Report does not exist"
        return jsonify(response)
    else:
        response['status'] = "Complete"
        with open(str(report_id) + '.csv', 'rb') as report_file:
            response['report.csv'] = base64.b64encode(report_file.read()).decode('utf-8')

        return jsonify(response)


app.run(debug=True)
