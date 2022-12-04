"""
Модуль, реализующий внешнюю сортировку естественным слиянием.

Функции:
    - sort_hub
    - split_series
    - _natural_merge
"""

import os
import pathlib
import shutil
from threading import Thread
from typing import Callable, Optional, Union

from external_sort.threads import Threads
from external_sort.reader import Reader, StrTup, ReaderGen


def sort_hub(src: list[str], type_data: StrTup = None,
             output: Optional[str] = None, reverse: bool = False,
             nflows: Optional[int] = None, cmp: Optional[Callable] = None,
             keys: Optional[tuple[str, ...]] = None, delimiter: str = ',') \
    -> Optional[int]:
    """
    Функция, обрабатывающая переданные на сортировку файлы, определяет их
        расширения, по необходимости запускает многопоточную сортировку.

    :param src: список src файлов
    :param type_data: кортеж или строка ожидаемых данных
    :param output: выходной файл
    :param reverse: флаг обратной сортировки (по невозрастанию)
    :param nflows: число потоков для сортировки
    :param cmp: функция, выполняющая сравнение
    :param keys: кортеж или строка ключей (для .csv файлов)
    :param delimiter: разделитель (для .csv файлов)
    """

    def _equals() -> None:
        """
        Функция, запускающая сортировку каждого из src файлов в своем отдельном
            потоке.
        Вызывается, если nflows соответствует числу переданных src или nflows
            не указано вовсе.
        """

        for i, s in enumerate(src):
            Threads.tasks[s] = \
                Thread(target=split_series,
                       args=(src[i], type_data, Reader(delimiter, src[i]),
                             output, reverse, cmp, keys))
        Threads.bound_workers()

    # Проверка расширений полученных файлов
    Reader.check_extension(src, output)

    # Создание словаря потоков
    Threads.tasks = dict.fromkeys(src, False)

    # Приведение ключей к кортежу (для тестов)
    if keys and not isinstance(keys, tuple):
        keys = (keys, )

    # Из cli type_data - tuple. В .txt моде возможен только один тип
    if len(type_data) > 1 and Reader.ext == '.txt':
        raise ValueError("In .txt mode only one type_data parameter allowed")
    elif len(type_data) == 1 and Reader.ext == '.txt':
        type_data = type_data[0]

    # Вычисление числа задач (файлов для сортировки)
    total = len(src)

    if total == 1:
        # Если всего один src
        split_series(src[0], type_data, Reader(delimiter, src[0], output),
                     output, reverse, cmp, keys)
    elif output:
        # Если есть output и несколько src, невозожно всем
        #   потокам писать в данный output. Сперва сливаем все в один файл,
        #   затем сортируем этот файл.
        Reader(delimiter).merge_src(src, output, type_data, delimiter)
        split_series(output, type_data, Reader(delimiter, output), None,
                     reverse, cmp, keys)
        return
    elif nflows is None or nflows >= total:
        # Если несколько src и либо не задано число потоков, либо число
        #   потоков превышает число src
        _equals()
    else:
        # Если потоков дано меньше, чем src
        while not Threads.all_done():
            # Пока все src не отсортированы
            while Threads.workers() < nflows:
                # Пока не все выделенные потоки в работе
                # В цикле всегда 0 < работников <= nflows
                nxt = Threads.get_free()
                if nxt:
                    Threads.tasks[nxt] = Thread(target=split_series,
                                                args=(nxt, type_data,
                                                      Reader(delimiter, nxt),
                                                      output, reverse, cmp,
                                                      keys))
                else:
                    break
            Threads.bound_workers()


def split_series(src: str, type_data: StrTup, rdr: Reader,
                 output: Optional[str], reverse: bool, cmp: Optional[Callable],
                 keys: Optional[str]) -> None:
    """
    Функция, разбивающая исходный файл на два временных. Первая наибольшая
        неубывающая (невозрастающая) серия будет записана в tmp_files[0],
        вторая в tmp_files[1], третья снова в tmp_files[0] и т.д.

    :param src: исходный файл
    :param type_data: ожидаемый тип данных
    :param rdr: объект класса Reader
    :param output: выходной файл
    :param reverse: флаг обратной сортировки (по невозрастанию)
    :param cmp: функция, выполняющая сравнение
    :param keys: ключи (для .csv файлов)
    """

    cmp = cmp if cmp else lambda x, y, r: (x >= y) if r else (x <= y)

    def _natural_merge() -> None:
        """
        Функция, выполняющая слияние временных файлов, сравнивая элементы
            их серий.
        """

        def _remain_all(cur: Union[int, float, str], gen: ReaderGen,
                        which: int) -> None:
            """
            Функция, выполняющая дозапись всего "противоположного" временного
                файла в выходной файл.
            Вызывается в случае, когда последняя серия в tmp_files[0](или 1)
                закончилась, но "противоположный" файл tmp_files[1](или 0
                соответсвенно) еще не дописан.

            :param cur: текущий элемент в буффере b или c
            :param gen: генератор файла, который должен переписан до конца
            :param which: индекс буффера для .csv файлов, который хранится в
                rdr
            """

            rdr.write_line(rdr.out_file, cur, which)
            for num in gen:
                rdr.write_line(rdr.out_file, num, which)

        def _remain_series(cur: Union[int, float, str], gen: ReaderGen,
                           which: int) -> \
            tuple[Optional[Union[int, float, str]], bool]:
            """
            Функция, выполняющая дозапись текущей серии "противоположного"
                временного файла в выходной файл.
            Вызывается в случае, когда текущая серия в tmp_files[0](или 1)
                закончилась, но "противоположный" файл tmp_files[1](или 0
                соответсвенно) еще не дописал свою серию.

            :param cur: текущий элемент в буффере b или c
            :param gen: генератор файла, серия в котором еще не дописана
            :param which: индекс буффера для .csv файлов, который хранится в
                rdr
            :return: элемент, с которого начинается следующая серия в
                "противоположном" файле (или None, если это была последняя
                 серия) и False для флага c_await или b_await
            """

            rdr.write_line(rdr.out_file, cur, which)
            for num in gen:
                if not cmp(cur, num, reverse):
                    return num, False
                rdr.write_line(rdr.out_file, num, which)
            return None, False  # Если переписан весь генератор

        rdr.open_tmp_r()
        rdr.open_out_w()

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
        rdr.tear_down()
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
        rdr.first_iteration = False
        _natural_merge()
    else:
        # Если разбиение не случилось => файл уже отсортирован
        # Копирование производится, если после первого разбиения выяснилось,
        #   что файл уже отсортирован, т.к. в таком случае в output ничего еще
        #   не было записано.
        if rdr.first_iteration and output and os.path.isfile(output):
            shutil.copy2(rdr.tmp_path[0], output)
        elif rdr.first_iteration and output and not os.path.isfile(output):
            pathlib.Path(output).touch()
            shutil.copy2(rdr.tmp_path[0], output)
        rdr.delete_tmp()
        Reader.delete_dir()
        Threads.done(src)  # данный поток закончил сортировку
        return
