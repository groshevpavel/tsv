"""
tsv.py
Обертка над csv.py для упрощения работы с CSV/TSV файлами

Позволяет:
    - читать CSV/TSV из файла или текста(строки в памяти)
    - писать CSV/TSV в файл или текст(строку в памяти)

tsv(параметры) - создать экземпляр класса

параметры:
    dialect: str = 'excel-tab' - название диалекта из модуля csv,
        если задать None то будет произведена попытка автоопределения
    filename: str = None - имя файла или полный путь + имя файла,
        не задается если задан text
    text: str = None - строка источник чтения CSV/TSV или строка для записи результата конвертации в CSV/TSV,
        не задается если задан filename
    headers: list[str] - список заголовков столбцов,
        задается в случае если исходный файл их не содержит и предполагается возврат строк файла в виде словарей
    headers_is_first_line: bool = True - флаг, что заголовки содержатся в первой строке, и их нужно прочитать оттуда
    headers_process_by: t.Callable = None - процедура для проверки заголовков,
        в случае ошибки проверки заголовков должна выбросить исключение


tsv(tsv_filename).reader() - вернуть csv-ридер https://docs.python.org/3/library/csv.html#csv.reader

    with tsv(tsv_filename).reader() as reader:
        for row in reader:
            print(row)


ЧТЕНИЕ ФАЙЛА:
    прочитать содержимое tsv_filename
    с учетом что файл не содержит заголовков столбцов (они задаются через параметр headers)
    и вернуть строки файла в виде словаря значений:

        for r in tsv(filename=tsv_filename, headers=['column1', 'column2']).readlines(asdict=True):
            print(r)   # {'column1': 'value1', 'column2': 'value2'}


    вернуть строки файла в виде списка значений:

        for r in tsv(filename=tsv_filename).readlines():
            print(r)   # ['value1', 'value2']

    При создании экземпляра класса tsv будет произведена проверка наличия файла,
    в случае отсутствия файла, перед началом чтения будет выброшено исключение FileNotFoundError


    вернуть строки с предварительной обработкой указанной функцией:

        for r in tsv(filename=tsv_filename).readlines(process_by=lambda row: row[-1]):
            print(r)   # вернет ['value1'] вместо фактического содержимого в виде: ['value1', 'value2']


ЧТЕНИЕ (текст в памяти):

        tsv_source = "base      params  basekey1        basevalue1\nbase        params  basekey2        basevalue2"
        rows_ = tsv(text=tsv_source).readlines()
        assert rows_ ==  [['base', 'params', 'basekey1', 'basevalue1'], ['base', 'params', 'basekey2', 'basevalue2']]


ЗАПИСЬ ФАЙЛА:
    записать содержимое rows_ в файл,
    rows_ список списков(или кортежей) значений строк будущего файла,
    результирующий файл не будет содержать строки заголовков столбцов!

        tsv(filename=tsv_write_filename).writelines(rows_, overwrite=True)

    rows_ - список(кортеж) словарей значений,
    результирующий файл содержит заголовки столбцов

        tsv(filename=tsv_write_filename).writelines(rows_, overwrite=True, asdict=True)

    В случае если производится попытка записи в существующий файл,
    будет выброшено исключение FileExistsError
    разрешить перезапись существующего файла можно указанием overwrite=True

    Перед записью производится проверка структуры rows_,
    сама структура должна быть списком или кортежем,
    внутри структуры должны быть списки, кортежи или словари,
    все элементы структуры должны быть такого же типа как первый элемент структуры

    Так же, в случае отсутствия заданных значений названий заголовков, параметр headers
    и в случае если вложенные структуры словари, производится попытка установить названия заголовков
    по значениям ключей первого из словарей структуры rows_

ЗАПИСЬ (текст в памяти):

        tsv_source = "base      params  basekey1        basevalue1\nbase        params  basekey2        basevalue2"
        rows_ = tsv(text=tsv_source).readlines()

        result = tsv(text="").writelines(rows_, overwrite=True)
        assert result == "base  params  basekey1        basevalue1\nbase        params  basekey2        basevalue2"

"""

import csv
import io
import sys
import typing as t
from contextlib import contextmanager
from pathlib import Path


DETECT_READ_BYTES_NUM: int = 1024


class Error(Exception):
    pass


class tsv:
    def __init__(  # noqa:CFQ002
            self,
            dialect: str = 'excel-tab',
            filename: t.Union[str, Path] = None,
            text: str = None,
            headers: t.List[str] = None,
            headers_is_first_line: bool = True,
            headers_process_by: t.Callable = None,
    ):
        if filename and text:
            raise Error('You must provide "filename" or "text" only!')
        if filename is None and text is None:
            raise Error('You did not provide "filename" or "text"!')

        self.filename = filename
        self.file = None
        self.file_is_exists = False

        if filename is not None:
            self.file: Path = Path(self.filename)
            self.file_is_exists = self.file.exists()

        self.text = text

        if text is not None:
            if isinstance(text, str):
                self.text = io.StringIO(self.text)
            elif isinstance(text, io.StringIO):
                self.text = text
            else:
                raise TypeError('text= value must contain str or io.StringIO instance')
            self.file_is_exists = True

        self.dialect = dialect
        if not dialect:
            self.dialect = self._detect_dialect()

        self.headers = headers
        self.headers_is_first_line = headers_is_first_line
        self.headers_process_by = headers_process_by

    @property
    def has_headers(self):
        return bool(self.headers)

    def _check_file_exists(self):
        if not self.file_is_exists:
            raise FileNotFoundError(self.filename)

    def _detect_dialect(self) -> t.Type[t.Union[csv.Dialect, csv.Dialect]]:
        with self.open() as tsvfile:
            tsvfile.seek(0)
            dialect = csv.Sniffer().sniff(tsvfile.read(DETECT_READ_BYTES_NUM))
            tsvfile.seek(0)

        self.dialect = dialect
        return dialect

    def _detect_headers(self, reader):
        if self.has_headers:
            return
        if not self.headers_is_first_line:
            return

        if getattr(reader, 'fieldnames', None):
            self.headers = reader.fieldnames
            return

        headers = None
        while not headers:  # skip empty tsv lines
            headers = next(reader)

        if not headers:
            raise Error('Absolutely empty file - no data lines!')

        if isinstance(headers, dict):
            # at now: reader.fieldnames is [], and we read first line as
            # OrderedDict([(None, ['date', 'off', 'block', 'section', 'key', 'val'])])
            # where headers is list at None key
            self.headers = headers[None]
            reader.fieldnames = self.headers
        elif isinstance(headers, (list, tuple, set)):
            self.headers = headers
        else:
            raise Error(f'Inappropriate headers format! Expect list, tuple, set, dict but has: {repr(headers)}')

    def _detect_headers_before_write(self, rows: t.Union[list, tuple, t.Generator]) -> tuple:
        first_row = next(rows) if isinstance(rows, t.Generator) else rows[0]

        if not isinstance(first_row, dict):
            raise ValueError('Can not detect headers because 1st row not a dict, supply headers manually')

        return tuple(first_row.keys()), first_row

    def _process_headers(self, reader):
        if not self.headers_process_by:
            return
        if not callable(self.headers_process_by):
            return

        self.headers = self.headers_process_by(self.headers)

        reader_fieldnames = getattr(reader, 'fieldnames', None)
        if reader_fieldnames:
            if reader_fieldnames != self.headers:
                reader.fieldnames = self.headers

    @contextmanager
    def open(self, **kwargs) -> io.TextIOWrapper:
        # if no newline in kwargs update it with newline:''
        required_params = {
            'newline': '',
            'encoding': 'utf-8',
        }
        required_params.update(kwargs) if kwargs else required_params

        if self.text is not None:
            yield self.text

        if self.filename is not None:
            with self.file.open(**required_params) as f:
                yield f

    @contextmanager
    def reader(self, **kwargs) -> csv.reader:
        self._check_file_exists()

        asdict = kwargs.pop('asdict', False)
        reader_kwargs = kwargs.pop('reader', {})

        if asdict and self.has_headers:
            reader_kwargs['fieldnames'] = self.headers

        with self.open(**kwargs) as tsvfile:
            if asdict:
                yield csv.DictReader(tsvfile, dialect=self.dialect, **reader_kwargs)
            else:
                yield csv.reader(tsvfile, dialect=self.dialect, **reader_kwargs)

    @contextmanager
    def writer(self, **kwargs) -> csv.writer:
        overwrite: bool = kwargs.pop('overwrite', False)

        if self.file_is_exists and not overwrite:
            raise FileExistsError(
                f'Attempt to overwrite existing file: "{self.filename}", use "overwrite=True"',
            )

        open_params = {
            'mode': 'w',
        }
        open_params.update(kwargs.pop('mode', {}))

        asdict = kwargs.pop('asdict', False)
        writer_kwargs = kwargs.pop('writer', {})
        if asdict and self.has_headers:
            writer_kwargs = {**{'fieldnames': self.headers}, **writer_kwargs}

        with self.open(**open_params) as tsvfile:
            if asdict:
                yield csv.DictWriter(tsvfile, dialect=self.dialect, **writer_kwargs)
            else:
                yield csv.writer(tsvfile, dialect=self.dialect, **writer_kwargs)

    def readlines(self, **kwargs) -> t.Generator:
        process_by = kwargs.pop('process_by', None)
        process_by = process_by if (process_by is not None and callable(process_by)) else None

        with self.reader(**kwargs) as reader:
            self._detect_headers(reader)

            self._process_headers(reader)

            try:
                for line in reader:
                    if process_by:
                        line = process_by(line, reader.line_num, self.filename)

                    # if line is None, it is marks that this line needs to be skipped
                    if line is None:
                        continue

                    yield line
            except csv.Error as e:
                sys.exit('File read error: file {}, line {}: {}'.format(self.filename, reader.line_num, e))

    def writelines(self, rows: t.Union[list, tuple, t.Generator], **kwargs):
        asdict = kwargs.get('asdict')
        first_line = None

        if asdict and not self.has_headers:
            self.headers, first_line = self._detect_headers_before_write(rows)

            if not first_line:
                raise Error(f'After header detection from rows= first_line has no data!\n{first_line}')

        with self.writer(**kwargs) as writer:
            # writeheader
            if not self.has_headers and isinstance(writer, csv.DictWriter):
                raise csv.Error('No headers set - write prohibited! Try to set asdict=True')

            if self.has_headers and isinstance(writer, csv.DictWriter):
                writer.writeheader()

            if isinstance(rows, t.Generator):
                # at headers detection phase, from rows=, as generator,
                # already was read first line of creating tsv-file -
                # save it additionally from first_line, right after header
                writer.writerow(first_line)

            for line in rows:
                writer.writerow(line)

        if isinstance(self.text, io.StringIO):
            return self.text.getvalue()
