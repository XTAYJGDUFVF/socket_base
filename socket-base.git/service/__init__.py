
import asyncio

from util.util import Singleton, RepeatTask


_task_settings = [
    # (30, report_status),
]


class Service(Singleton):

    def __init__(self):

        self._task_settings = []

    def load_settings(self, task_settings):

        self._task_settings = task_settings

    def run(self):

        for task_info in self._task_settings:

            report_task = RepeatTask(task_info[0], task_info[1])
            loop = asyncio.get_event_loop()
            coro = report_task.run()
            loop.create_task(coro)

Service().load_settings(_task_settings)
