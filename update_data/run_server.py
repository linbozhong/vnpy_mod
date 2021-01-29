import multiprocessing
from time import sleep
from datetime import datetime, time
from logging import INFO

from server import DataRpcServer


def run_child():
    """
    Running in the child process.
    """
    server_ins = DataRpcServer()
    server_ins.run_server()

    while True:
        sleep(1)


def run_parent():
    """
    Running in the parent process.
    """
    print("启动数据更新守护进程")

    # Chinese futures market trading period (day/night)
    A_START = time(3, 0)
    A_END = time(8, 45)

    B_START = time(15, 0)
    B_END = time(20, 45)

    child_process = None

    while True:
        current_time = datetime.now().time()
        trading = False

        # Check whether in trading period
        if (
            (current_time >= A_START and current_time <= A_END)
            or (current_time >= B_START and current_time <= B_END)
        ):
            trading = True

        # Start child process in trading period
        if trading and child_process is None:
            print("启动子进程")
            child_process = multiprocessing.Process(target=run_child)
            child_process.start()
            print("子进程启动成功")

        # 非记录时间则退出子进程
        if not trading and child_process is not None:
            print("关闭子进程")
            child_process.terminate()
            child_process.join()
            child_process = None
            print("子进程关闭成功")

        sleep(5)


if __name__ == "__main__":
    run_parent()
