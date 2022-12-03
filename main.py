import csv
import os
import pathlib
import shutil
import string
import random
import traceback
from threading import Thread
from typing import TextIO, Callable, Optional, Union, Generator

StrTup = Union[tuple[str, ...], str]
ReaderGen = Generator[Union[int, float, str], None, None]

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
    tasks = dict()

    @classmethod
    def get_free(cls) -> str:
        """
        Метод, возвращающий свободную в данный момент задачу.

        :return: строку-имя задачи (путь до src)
        """

        for task, status in cls.tasks.items():
            if status == False:
                return task
        return None

    @classmethod
    def all_done(cls) -> bool:
        """
        Метод, проверяющий выполнены ли все задачи.

        :return: True - все задачи выполнены, False - в противном случае
        """

        for status in cls.tasks.values():
            if status != True:
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


class Reader:
    """
    Класс, реализующий унифицированные методы последовательного чтения и записи
        .txt и .csv файлов.

    Поля:
        - ext
        - src_path
        - out_path
        - tmp_path
        - out_file
        - tmp_files
        - first_split
        - delimiter_csv
        - header_csv
        - files_w_header
        - buf_row_csv

    Методы:
        - __init__
        - check_extension
        - open_tmp_w
        - open_tmp_r
        - open_out_w
        - open_src_r
        - close_all
        - delete_tmp
        - write_line
        - create_tmp_generators
        - _cast
        - _content_error
        - _txt_gen
        - _csv_gen
        - generator
        - _txt_merge
        - _csv_merge
        - merge_src
    """

    def __init__(self, ext: str, delimiter: str = ',', src: str = None) \
        -> None:
        """
        Параметризованный конструктор класса Reader.

        :param ext: расширение src файла
        :param delimiter: разделитель для .csv файла
        :param src: src файл
        """

        self.ext = ext

        self.src_path = src
        self.out_path = None
        self.tmp_path = [
            "temp/" + ''.join(random.choices(string.ascii_uppercase +
                                             string.digits, k=10)) + ext,
            "temp/" + ''.join(random.choices(string.ascii_uppercase +
                                             string.digits, k=10)) + ext
        ]

        self.out_file = None
        self.tmp_files = []

        self.first_split = True

        self.delimiter_csv = delimiter
        self.header_csv = None
        self.files_w_header = []
        self.buf_row_csv = {0: None, 1: None}

    @staticmethod
    def check_extension(src: str, out: str) -> str:
        """
        Метод, проверяющий расширения полученных файлов.

        :param src: src файл
        :param out: output файл
        :return: расширение, если оно прошло проверку
        """

        ext = os.path.splitext(os.path.basename(src[0]))[1]
        if ext not in (".txt", ".csv"):
            raise ValueError("Incorrect file type given")
        for file in src[1:] + [out]:
            if file and not file.endswith(ext):
                raise ValueError(f"Each file must be {ext}")
        # Если расширение прошло проверку
        return ext

    def open_tmp_w(self) -> None:
        """
        Метод, открывающий временные файлы для записи. Имена временных файлов
            имеют то же расширение, что и переданные src. Длина tmp файлов - 10
            случайных символов.
        """

        self.tmp_files = [open(i, 'w', newline='') for i in self.tmp_path]

    def open_tmp_r(self):
        """Метод, открывающий временные файлы для чтения."""

        self.tmp_files = [open(i, 'r') for i in self.tmp_path]

    def open_out_w(self, out: Optional[str]) -> None:
        """
        Метод, открывающий выходной файл на запись.

        :param out: путь до выходного файла
        """

        if out:
            # Если был передан output, откроется он, иначе открывается src
            self.out_file = open(out, 'w', newline='')
        else:
            self.out_file = open(self.src_path, 'w', newline='')

    def open_src_r(self) -> None:
        """Метод, отркывающий исходный файл на чтение."""

        self.out_file = open(self.src_path, 'r')

    def close_all(self) -> None:
        """Метод, закрывающий все открытые файлы ридера."""

        if self.tmp_files:
            self.tmp_files = [i.close() if not i.closed else i for i in
                              self.tmp_files]
        if self.out_file:
            self.out_file = self.out_file.close()

    def delete_tmp(self) -> None:
        """Метод, удаляющий временные файлы."""

        if self.tmp_files:
            for file in self.tmp_path:
                os.remove(file)

    def write_line(self, file: TextIO, elem: Union[str, int, float],
                   which: int = 0) -> None:
        """
        Унифицированный метод для записи новой строки в .txt/.csv файл.

        :param file: уже открытй файл для записи
        :param elem: значение для записи (для .txt)
        :param which: ключ файла, в который необходимо записать строку
            (для .csv)
        """

        if self.ext == ".txt":
            file.write(str(elem) + '\n')
        else:
            writer = csv.DictWriter(file, self.header_csv, delimiter=self.delimiter_csv)
            if file not in self.files_w_header:
                writer.writeheader()
                self.files_w_header.append(file)
            writer.writerow(self.buf_row_csv[which])

    def create_tmp_generators(self, type: StrTup, keys: tuple[str, ...]) \
        -> tuple[ReaderGen, ...]:
        """
        Метод, возвращающий генераторы для tmp файлов.

        :param type: тип содержимого файлов
        :param keys: ключи для сортировки (для .csv)
        :return: кортеж генераторов
        """

        return self.generator(open(self.tmp_path[0], "r"), type, keys, 0), \
               self.generator(open(self.tmp_path[1], "r"), type, keys, 1)

    @staticmethod
    def _cast(item: str, type: str) -> Union[int, float, str]:
        """
        Метод, приводящий значение из файла к необходимому типу.

        :param item: полученная из файла строка
        :param type: ожидаемый тип
        :return: приведенная к нужному типу строка
        """

        if type == 'i':
            return int(item)
        elif type == 's':
            return item.replace('\n', '')
        elif type == 'f':
            return float(item)

    def _content_error(self) -> None:
        """Метод обрабатывающий исключение, возникающее в методе _cast."""

        self.close_all()
        self.delete_tmp()
        print(traceback.format_exc() + "\nFile contents do not conform"
                                       " given type.")
        exit(1)

    def _txt_gen(self, f: TextIO, type: StrTup) -> ReaderGen:
        """
        Генератор для .txt файлов.

        :param f: уже открытый файл для чтения
        :param type: ожидаемый тип данных
        :return: генератор
        """

        for line in f.readlines():
            try:
                yield self._cast(line, type)
            except ValueError:
                self._content_error()
        f.close()

    def _csv_gen(self, f, type: StrTup, keys: tuple[str, ...], which: int) -> \
        ReaderGen:
        """
        Генератор для .csv файлов.

        :param f: уже открытый файл для чтения
        :param type: ожидаемые типы данных по ключам
        :param keys: ключи для .csv файла
        :param which: ключ для словаря, хранящего значения генераторов tmp
            файлов (buf_row_csv)
        :return: генератор
        """

        # Если типов меньше или больше чем ключей => ValueError
        # Если тип всего 1 => каждое поле будет приводится к нему
        if len(type) == 1:
            type = [type[0] for _ in keys]
        elif len(type) < len(keys) or len(type) > len(keys):
            raise ValueError(f"Number of types is not equal to number of keys")

        for line in csv.DictReader(f, delimiter=self.delimiter_csv):
            try:
                to_yield = []
                i = 0  # индекс типа, к которому необходимо сделать каст
                for key, val in line.items():
                    if key in keys:
                        to_yield.append(self._cast(val, type[i]))
                        i += 1
                if which == 0:
                    self.buf_row_csv[0] = line
                else:
                    self.buf_row_csv[1] = line
                if self.header_csv is None:
                    self.header_csv = list(line.keys())
                    for key in keys:
                        if key not in self.header_csv:
                            raise KeyError(f"Can't find given key: {key}")
                yield to_yield
            except ValueError:
                self._content_error()
        f.close()

    def generator(self, f: TextIO, type: StrTup,
                  keys: Optional[tuple[str, ...]] = None, which: int = 0) -> \
        ReaderGen:
        """
        Унифицированный метод, возвращающий генератор в соответствии с типом
            src.

        :param f: уже открытый файл
        :param type: ожидаемый тип данных
        :param keys: кортеж ключей для .csv
        :param which: индекс для tmp файла (для .csv)
        :return: соответствующий генератор
        """

        if self.ext == ".txt":
            return self._txt_gen(f, type)
        else:
            # Обнуление списка файлов, в которых уже есть хедер
            self.files_w_header = []
            return self._csv_gen(f, type, keys, which)

    @staticmethod
    def _txt_merge(src: list[str], out: str, type: StrTup, ext: str) -> None:
        """
        Метод, сливающий все src.txt в один output, если src несколько.

        :param src: список исходных файлов
        :param out: выходной файл
        :param type: ожидаемый тип данных
        :param ext: расширение файла
        """

        with open(out, 'w') as o:
            for i in range(len(src)):
                if os.stat(src[i]).st_size == 0:
                    continue
                with open(src[i], "r") as s:
                    generator = Reader(ext).generator(s, type)
                    for line in generator:
                        o.write(str(line) + '\n')

    @staticmethod
    def _csv_merge(src: list[str], out: str, type: StrTup, ext: str,
                   delimiter: str) -> None:
        """
        Метод, сливающий все src.csv в один output, если src несколько.

        :param src: список исходных файлов
        :param out: выходной файл
        :param type: ожидаемый тип данных
        :param ext: расширение файла
        :param delimiter: разделитель
        """

        with open(src[0], 'r') as f:
            # Получение общего хедера
            header = list(next(csv.DictReader(f, delimiter=delimiter)).keys())
        with open(out, 'w', newline='') as o:
            writer = csv.DictWriter(o, header, delimiter)
            writer.writeheader()
            for i in range(len(src)):
                if os.stat(src[i]).st_size == 0:
                    continue
                with open(src[i], 'r') as s:
                    reader = csv.DictReader(s, delimiter=delimiter)
                    try:
                        if list((first := next(reader)).keys()) != header:
                            raise ValueError("Src files have different "
                                             "headers")
                        writer.writerow(first)
                        for row in reader:
                            writer.writerow(row)
                    except StopIteration:
                        continue

    def merge_src(self, src: list[str], out: str, type: StrTup,
                  delimiter: str) -> None:
        """
        Унифицированный метод, вызывающий методы слияния src файлов в один
            output.

        :param src: исходные файлы
        :param out: выходной файл
        :param type: ожидаемый тип данных
        :param delimiter: разделитель для .csv
        """

        if self.ext == ".txt":
            self._txt_merge(src, out, type, self.ext)
        else:
            self._csv_merge(src, out, type, self.ext, delimiter)


def natural_merge(src: list[str], type_data: Union[str, tuple[str, ...]],
                  output: Optional[str] = None, reverse: bool = False,
                  nflows: int = None, cmp: Optional[Callable] = None,
                  keys: Optional[tuple[str, ...]] = None,
                  delimiter: str = ','):

    def _equals():
        for i, s in enumerate(src):
            Threads.tasks[s] = \
                Thread(target=split_series,
                       args=(src[i], type_data, Reader(ext, delimiter, src[i]),
                             output, reverse, cmp, keys))
        Threads.bound_workers()

    # Проверка расширений полученных файлов
    ext = Reader.check_extension(src, output)

    # Создание словаря потоков
    Threads.tasks = dict.fromkeys(src, False)

    # Вычисление числа задач (файлов для сортировки)
    total = len(src)

    if total == 1:
        # Если всего один src
        split_series(src[0], type_data, Reader(ext, delimiter, src[0]), output,
                     reverse, cmp, keys)
    elif output:
        # Если есть output и несколько src, невозожно всем
        #   потокам писать в данный output. Сперва сливаем все в один файл,
        #   затем сортируем этот файл.
        Reader(ext, delimiter).merge_src(src, output, type_data, delimiter)
        split_series(output, type_data, Reader(ext, delimiter, output), None,
                     reverse, cmp, keys)
        return
    elif nflows is None or nflows >= total:
        # Если несколько src и либо не задано число потоков, либо число потоков
        #   превышает число src
        _equals()
    else:
        # Если потоков дано меньше, чем src
        while not Threads.all_done():
            # Пока все src не отсортированы
            while Threads.workers() < nflows:
                # Пока не все выделенные потоки в работе
                # В цикле всегда 0 < работников <= nflows
                next = Threads.get_free()
                Threads.tasks[next] = Thread(target=split_series,
                                             args=(next, type_data,
                                                   Reader(ext, delimiter,
                                                          next),
                                                   output, reverse, cmp, keys))
            Threads.bound_workers()


def split_series(src: str, type_data: str, rdr: Reader, output: Optional[str],
                 reverse: bool, cmp: Optional[Callable], keys: Optional[str]):

    cmp = cmp if cmp else lambda x, y, r: (x >= y) if r else (x <= y)

    def _natural_merge():

        def _remain_all(cur, gen, which):
            rdr.write_line(rdr.out_file, cur, which)
            for num in gen:
                rdr.write_line(rdr.out_file, num, which)

        def _remain_series(cur, gen, which):
            rdr.write_line(rdr.out_file, cur, which)
            for num in gen:
                if not cmp(cur, num, reverse):
                    return num, False
                rdr.write_line(rdr.out_file, num, which)
            return None, False  # Если переписан весь генератор

        rdr.open_tmp_r()
        rdr.open_out_w(output if output else None)

        b_generator, c_generator = rdr.create_tmp_generators(type_data, keys)
        b_buf = next(b_generator)
        c_buf = next(c_generator)
        b_await = c_await = False

        while True:
            if b_await:
                # Если в b закончилась серия
                c_buf, b_await = _remain_series(c_buf, c_generator, which=1)
                if c_buf is not None:
                    continue
                else:
                    _remain_all(b_buf, b_generator, which=0)
                    break
            if c_await:
                # Если в c закончилась серия
                b_buf, c_await = _remain_series(b_buf, b_generator, which=0)
                if b_buf is not None:
                    continue
                else:
                    _remain_all(c_buf, c_generator, which=1)
                    break
            if cmp(b_buf, c_buf, reverse):
                # Запись элемента текущей серии в b
                rdr.write_line(rdr.out_file, b_buf, which=0)
                try:
                    if not cmp((b_prev := b_buf), (b_buf := next(b_generator)),
                               reverse):
                        b_await = True
                except StopIteration:
                    _remain_all(c_buf, c_generator, which=1)
                    break
            else:
                # Запись элемента текущей серии в c
                rdr.write_line(rdr.out_file, c_buf, which=1)
                try:
                    if not cmp((c_prev := c_buf), (c_buf := next(c_generator)),
                               reverse):
                        c_await = True
                except StopIteration:
                    _remain_all(b_buf, b_generator, which=0)
                    break

        rdr.close_all()

        split_series(src, type_data, rdr, output, reverse, cmp, keys)

    rdr.open_tmp_w()
    rdr.open_src_r()

    row_generator = rdr.generator(rdr.out_file, type_data, keys)
    try:
        buf = next(row_generator)
    except StopIteration:
        rdr.close_all()
        rdr.delete_tmp()
        return  # Пустой файл игнорируется (для тестов)

    rdr.write_line(rdr.tmp_files[0], buf)
    which = 0
    splitted = False

    for elem in row_generator:
        if not cmp(buf, elem, reverse):
            which = 1 - which
            splitted = True
        rdr.write_line(rdr.tmp_files[which], elem)
        buf = elem

    rdr.close_all()

    if splitted:
        # Если произошло разбиение
        rdr.first_split = False
        _natural_merge()
    else:
        # Если разбиение не случилось => файл уже отсортирован
        # Копирование производится, если после первого разбиения выяснилось,
        #   что файл уже отсортирован, т.к. в таком случае в output ничего еще
        #   не было записано.
        if rdr.first_split and output and os.path.isfile(output):
            shutil.copy2(rdr.tmp_path[0], output)
        elif rdr.first_split and output and not os.path.isfile(output):
            pathlib.Path(output).touch()
            shutil.copy2(rdr.tmp_path[0], output)
        rdr.delete_tmp()
        Threads.done(src)  # данный поток закончил сортировку
        return
