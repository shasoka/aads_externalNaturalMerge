"""
Модуль, реализующий класс Reader.

Содержит переменные для аннотации аргументов:
    - StrTup - строка или кортеж строк
    - ReaderGen - генератор полученный из ридера
"""

import ast
import os
import csv
import string
import random
from typing import TextIO, Union, Optional, Generator


StrTup = Optional[Union[tuple[str, ...], str]]
ReaderGen = Generator[Union[int, float, str], None, None]


class Reader:
    """
    Класс, реализующий унифицированные методы последовательного чтения и записи
        .txt и .csv файлов.

    Поля класса:
        - ext - расширение сортируемых и выходных файлов

    Поля объекта класса:
        - src_path - путь до src
        - out_path - путь до output
        - tmp_path - пути до временных файлов
        - out_file - враппер выходного файла
        - tmp_files - врапперы временных файлов
        - first_iteration - флаг первого открытия src (если есть output,
            src_path заменится out_path после первого открытия)
        - delimiter_csv - разделитель для .csv файлов
        - header_csv - список имен столбцов для .csv файла
        - has_header - список файлов, в которые уже был записан хедер
        - buf_row_csv - словарь-буфер хранящий строки csv таблицы, которые
            соответсвуют последнему возвращенному значению генератора

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
        - tear_down
        - _txt_gen
        - _csv_gen
        - generator
        - _txt_merge
        - _csv_merge
        - merge_src
    """

    # Общее расщирение файлов.
    ext = None

    def __init__(self, delimiter: str = ',', src: str = None, out: str = None)\
        -> None:
        """
        Параметризованный конструктор класса Reader.

        :param delimiter: разделитель для .csv файла
        :param src: src файл
        :param out: output файл
        """

        def _tmp_name() -> str:
            """
            Функция, генерирующая случайное имя для временного файла.

            :return: сгенерированная строка
            """

            return "temp/" + \
                ''.join(random.choices(
                    string.ascii_uppercase + string.digits, k=10)) + \
                   self.ext

        self.src_path = src
        self.out_path = out
        self.out_file = None
        self.tmp_path = [_tmp_name() for _ in range(2)]
        self.tmp_files = []

        self.first_iteration = True

        self.delimiter_csv = delimiter
        self.header_csv = None
        self.has_header = []
        self.buf_row_csv = {0: None, 1: None}

    @classmethod
    def check_extension(cls, src: str, out: str) -> str:
        """
        Метод, проверяющий расширения полученных файлов.

        :param src: src файл
        :param out: output файл
        :return: расширение, если оно прошло проверку
        """

        ext = os.path.splitext(os.path.basename(src[0]))[1]
        if ext not in (".txt", ".csv"):
            raise ValueError("Incorrect file dtype given")
        for file in src[1:] + [out]:
            if file and not file.endswith(ext):
                raise ValueError(f"Each file must be {ext}")
        # Если расширение прошло проверку
        cls.ext = ext

    def open_tmp_w(self) -> None:
        """
        Метод, открывающий временные файлы для записи. Имена временных файлов
            имеют то же расширение, что и переданные src. Длина tmp файлов - 10
            случайных символов.
        """

        if not os.path.exists("temp"):
            try:
                os.mkdir("temp")
            except FileExistsError:
                pass
        self.tmp_files = [open(i, 'w', newline='') for i in self.tmp_path]

    def open_tmp_r(self):
        """Метод, открывающий временные файлы для чтения."""

        self.tmp_files = [open(i, 'r') for i in self.tmp_path]

    def open_out_w(self) -> None:
        """Метод, открывающий выходной файл на запись."""

        self.out_file = open(self.src_path, 'w', newline='')

    def open_src_r(self) -> None:
        """Метод, отркывающий исходный файл на чтение."""

        self.out_file = open(self.src_path, 'r')
        if self.first_iteration and self.out_path:
            # Полсе первого прохода путь до сурс файла окажется не нужен,
            #   т.к. все исходные данные из него уже выписаны
            self.src_path = self.out_path

    def close_all(self) -> None:
        """Метод, закрывающий все открытые файлы ридера."""

        if self.tmp_files:
            self.tmp_files = [i.close() if i and not i.closed else i for i in
                              self.tmp_files]
        if self.out_file:
            self.out_file = self.out_file.close()

    def delete_tmp(self) -> None:
        """Метод, удаляющий временные файлы."""

        if self.tmp_files:
            for file in self.tmp_path:
                try:
                    os.remove(file)
                except (FileNotFoundError, OSError):
                    pass

    @staticmethod
    def delete_dir() -> None:
        """Функция, удаляющая временную папку."""

        try:
            os.rmdir("temp")
        except (FileNotFoundError, OSError):
            pass

    def tear_down(self) -> None:
        """
        Метод, вызываемый при экстренном завершении работы. Затирает следы
            работы скрипта.
        """

        self.close_all()
        self.delete_tmp()
        Reader.delete_dir()

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
            writer = csv.DictWriter(file, self.header_csv,
                                    delimiter=self.delimiter_csv)
            if file not in self.has_header:
                writer.writeheader()
                self.has_header.append(file)
            writer.writerow(self.buf_row_csv[which])

    def create_tmp_generators(self, dtype: StrTup, keys: tuple[str, ...]) \
        -> tuple[ReaderGen, ...]:
        """
        Метод, возвращающий генераторы для tmp файлов.

        :param dtype: тип содержимого файлов
        :param keys: ключи для сортировки (для .csv)
        :return: кортеж генераторов
        """

        return self.generator(open(self.tmp_path[0], "r"), dtype, keys, 0), \
            self.generator(open(self.tmp_path[1], "r"), dtype, keys, 1)

    def _cast(self, item: str, dtype: str) -> Union[int, float, str]:
        """
        Метод, приводящий значение из файла к необходимому типу.

        :param item: полученная из файла строка
        :param dtype: ожидаемый тип
        :return: приведенная к нужному типу строка
        """

        if dtype == 'i':
            return int(item)
        elif dtype == 's':
            if item:
                return item.replace('\n', '')
            self.tear_down()
            raise ValueError("File contents do not conform given dtype.")
        elif dtype == 'f':
            return float(item)

    @staticmethod
    def _auto_cast(string: str) -> str:
        """
        Метод, подбирающий тип данных для прочитанной строки, если изначально
            тип не был передан.

        :param string: прочитанная строка
        :return: код типа
        """

        try:
            expect = type(ast.literal_eval(string))
        except (ValueError, SyntaxError):
            expect = str
        return 'i' if expect == int else ('f' if expect == float else 's')

    def _txt_gen(self, f: TextIO, dtype: StrTup) -> ReaderGen:
        """
        Генератор для .txt файлов.

        :param f: уже открытый файл для чтения
        :param dtype: ожидаемый тип данных
        :return: генератор
        """

        for line in f.readlines():
            if not dtype:
                dtype = self._auto_cast(line)
            try:
                yield self._cast(line, dtype)
            except ValueError:
                self.tear_down()
                raise ValueError("File contents do not conform given dtype.")
        f.close()

    def _csv_gen(self, f, dtype: StrTup, keys: tuple[str, ...], which: int) ->\
        ReaderGen:
        """
        Генератор для .csv файлов.

        :param f: уже открытый файл для чтения
        :param dtype: ожидаемые типы данных по ключам
        :param keys: ключи для .csv файла
        :param which: ключ для словаря, хранящего значения генераторов tmp
            файлов (buf_row_csv)
        :return: генератор
        """

        def _auto_cast_csv() -> tuple[str, ...]:
            """
            Функция, определяющая типы для сортируемых полей, если типы
                не переданы.

            :return: список найденных предположительных типов
            """

            exp_types = []
            for key, val in line.items():
                print(line)
                if key in keys:
                    print(val)
                    exp_types.append(self._auto_cast(val))
            return tuple(exp_types)

        # if dtype and keys:
        if dtype and len(dtype) == 1:
            # Если тип всего 1 => каждое поле будет приводится к нему
            dtype = [dtype[0] for _ in keys]
        elif dtype and keys and (len(dtype) < len(keys) or len(dtype) >
                                 len(keys)):
            # Если типов меньше или больше чем ключей => ValueError
            self.tear_down()
            raise ValueError(f"Number of types is not equal to number of keys")

        for line in csv.DictReader(f, delimiter=self.delimiter_csv):
            if not keys:
                keys = tuple(line.keys())
            if not dtype:
                dtype = _auto_cast_csv()
            try:
                to_yield = []
                i = 0  # индекс типа, к которому необходимо сделать каст
                for key, val in line.items():
                    if key in keys:
                        to_yield.append(self._cast(val, dtype[i]))
                        i += 1
                if which == 0:
                    self.buf_row_csv[0] = line
                else:
                    self.buf_row_csv[1] = line
                if self.header_csv is None:
                    self.header_csv = list(line.keys())
                    for key in keys:
                        if key not in self.header_csv:
                            self.tear_down()
                            raise KeyError(f"Can't find given key: {key}. "
                                           f"Available keys: {self.header_csv}")
                yield to_yield
            except ValueError:
                self.tear_down()
                raise ValueError("File contents do not conform given dtype.")
        f.close()

    def generator(self, f: TextIO, dtype: StrTup,
                  keys: Optional[tuple[str, ...]] = None, which: int = 0) -> \
        ReaderGen:
        """
        Унифицированный метод, возвращающий генератор в соответствии с типом
            src.

        :param f: уже открытый файл
        :param dtype: ожидаемый тип данных
        :param keys: кортеж ключей для .csv
        :param which: индекс для tmp файла (для .csv)
        :return: соответствующий генератор
        """

        if self.ext == ".txt":
            return self._txt_gen(f, dtype)
        else:
            # Обнуление списка файлов, в которых уже есть хедер
            self.has_header = []
            return self._csv_gen(f, dtype, keys, which)

    @staticmethod
    def _txt_merge(src: list[str], out: str, dtype: StrTup, ext: str) -> None:
        """
        Метод, сливающий все src.txt в один output, если src несколько.

        :param src: список исходных файлов
        :param out: выходной файл
        :param dtype: ожидаемый тип данных
        :param ext: расширение файла
        """

        with open(out, 'w') as o:
            for i in range(len(src)):
                if os.stat(src[i]).st_size == 0:
                    continue
                with open(src[i], "r") as s:
                    generator = Reader(ext).generator(s, dtype)
                    for line in generator:
                        o.write(str(line) + '\n')

    @staticmethod
    def _csv_merge(src: list[str], out: str, dtype: StrTup, ext: str,
                   delimiter: str) -> None:
        """
        Метод, сливающий все src.csv в один output, если src несколько.

        :param src: список исходных файлов
        :param out: выходной файл
        :param dtype: ожидаемый тип данных
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

    def merge_src(self, src: list[str], out: str, dtype: StrTup,
                  delimiter: str) -> None:
        """
        Унифицированный метод, вызывающий методы слияния src файлов в один
            output.

        :param src: исходные файлы
        :param out: выходной файл
        :param dtype: ожидаемый тип данных
        :param delimiter: разделитель для .csv
        """

        if self.ext == ".txt":
            self._txt_merge(src, out, dtype, self.ext)
        else:
            self._csv_merge(src, out, dtype, self.ext, delimiter)
