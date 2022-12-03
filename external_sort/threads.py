"""
Модуль, реализующий класс Threads.
Класс Threads хранит словарь задач, их статус и активные потоки.
"""

from threading import Thread


class Threads:
    """
    Класс, отвечающий за создание и работу потоков, выполняющих сортировку
        параллельно.

    Поля:
        - tasks

    Методы:
        - get_free
        - all_done
        - workers
        - bound_workers
        - done
    """

    # False - задача не взята в работу
    # Thread - в работе
    # True - выполнена
    tasks = {}

    @classmethod
    def get_free(cls) -> str:
        """
        Метод, возвращающий свободную в данный момент задачу.

        :return: строку-имя задачи (путь до src)
        """

        for task, status in cls.tasks.items():
            if not status:
                return task
        return None

    @classmethod
    def all_done(cls) -> bool:
        """
        Метод, проверяющий выполнены ли все задачи.

        :return: True - все задачи выполнены, False - в противном случае
        """

        for status in cls.tasks.values():
            if not status:
                return False
        return True

    @classmethod
    def workers(cls) -> int:
        """
        Метод, возвращающий текущее число задач, взятых в работу, но еще не
            выполненных.

        :return: число работающих потоков
        """

        total = 0
        for status in cls.tasks.values():
            if isinstance(status, Thread):
                total += 1
        return total

    @classmethod
    def bound_workers(cls) -> None:
        """
        Метод, запускающий созданные потоки и применяющий к ним метод .join()
            для исключения асинхронности их выполнения.
        """

        # Сперва потоки запускаются
        for task, status in cls.tasks.items():
            if isinstance(status, Thread):
                status.start()
        # Затем синхронизируются с функцией natural_merge_hub
        for status in cls.tasks.values():
            if isinstance(status, Thread):
                status.join()

    @classmethod
    def done(cls, task: str) -> None:
        """
        Метод, устанавливающий полученной задаче статус True (выполнена).

        :param task: выполненная задача
        """

        cls.tasks[task] = True
