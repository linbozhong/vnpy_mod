#!/usr/bin/env python
import zmq
import time
from zmq.backend.cython.constants import NOBLOCK
KEEP_ALIVE_TOPIC = "_keep_alive"
from vnpy.trader.event import EVENT_ACCOUNT

from threading import Lock
from flask import Flask, render_template, session, request, \
    copy_current_request_context
from flask_socketio import SocketIO, emit, join_room, leave_room, \
    close_room, rooms, disconnect

async_mode = None

app = Flask(__name__)
app.config['SECRET_KEY'] = 'secret!'
socketio = SocketIO(app, async_mode=async_mode)
thread = None
thread_lock = Lock()


def background_thread():
    """Example of how to send server generated events to clients."""
    count = 0

    zmq_context = zmq.Context()
    zmq_req = zmq_context.socket(zmq.REQ)
    zmq_sub = zmq_context.socket(zmq.SUB)

    zmq_sub.setsockopt_string(zmq.SUBSCRIBE, "")

    zmq_req.connect("tcp://127.0.0.1:2014")
    zmq_sub.connect("tcp://127.0.0.1:4102")

    print("等待5秒，连接RPC接口")
    time.sleep(5)
    print("等待完成")

    while True:
        if not zmq_sub.poll(3000):
            print('nothing happen')
            continue

        topic, data = zmq_sub.recv_pyobj(flags=NOBLOCK)
        
        if topic == KEEP_ALIVE_TOPIC:
            continue

        if data is None:
            print("None data", topic, data)
            continue
        else:
            if data.type == EVENT_ACCOUNT:
                account_data = data.data
                account = {
                    'accountid': account_data.accountid,
                    'balance': account_data.balance,
                    'count': count
                }
        
                # socketio.sleep(10)
                count += 1
                socketio.emit('push_test',
                              account,
                              namespace='/test')

@app.route('/')
def index():
    return render_template('index.html', async_mode=socketio.async_mode)

@socketio.on('connect', namespace='/test')
def test_connect():
    global thread
    with thread_lock:
        if thread is None:
            thread = socketio.start_background_task(background_thread)
    emit('my_response', {'data': 'Connected', 'count': 0})

@socketio.on('disconnect', namespace='/test')
def test_disconnect():
    print('Client disconnected', request.sid)

if __name__ == '__main__':
    socketio.run(app, debug=True)
