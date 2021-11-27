import csv
import json
import os
import shutil
import sys
from tempfile import NamedTemporaryFile
from typing import Any, Dict, Iterable, Iterator, List, Optional, Set, Union

csv.field_size_limit(1024000)


def csv_reader(
    file_path: str,
    delimiter: str = '|',
    fieldnames: Optional[List[str]] = None,
    skip_lines: int = 0,
    encoding: str = 'utf-8-sig',
    fields: Optional[Set[str]] = None,
    use_maxsize: bool = False
) -> Iterator[Dict[str, str]]:
    "Returns a generator from CSV file in `file_path` as dicts."

    if fields is None or isinstance(fields, (frozenset, set)):
        pass
    elif isinstance(fields, (list, tuple)):
        fields = frozenset(fields)
    else:
        raise TypeError(
            f"Param \"fields\" expected a set, received {type(fields)}.")

    if use_maxsize:
        csv.field_size_limit(sys.maxsize)

    with open(file_path, encoding=encoding) as file_:
        lines = (line.replace('\0', '') for line in file_)

        for _ in range(skip_lines):
            next(lines, None)

        rows = csv.DictReader(
            lines,
            delimiter=delimiter,
            fieldnames=fieldnames
        )

        if fields:
            rows = ({field: row[field] for field in fields} for row in rows)

        yield from rows

    return


def csv_writer(
    dest_path: str,
    data_iter: Iterable[Dict[str, Any]],
    delimiter: str = '|',
    encoding: str = 'utf-8',
    use_maxsize: bool = False,
    shard_size: int = None,
    shard_num: int = None
) -> None:
    """Writes the `data_iter` in the `dest_path` as a CSV file, creating new
    columns as needed."""

    if use_maxsize:
        csv.field_size_limit(sys.maxsize)

    data_iter = iter(data_iter)
    first_row = next(data_iter, None)

    if not first_row:
        if not shard_size or (shard_num and shard_num < 1):
            print('No lines to write!')
        if shard_size:
            return None,None
        return

    count = 1
    rewrite = False
    fields = list(first_row.keys())
    file_tmp = NamedTemporaryFile(
        mode='w',
        prefix='sc_bin_',
        delete=False,
        encoding=encoding
    )

    with file_tmp:
        csv_writer = csv.DictWriter(
            file_tmp,
            fieldnames=fields,
            delimiter=delimiter
        )
        csv_writer.writeheader()

        print('Starting to write')

        csv_writer.writerow({
            k: _normalize_value(v)
            for k, v in first_row.items()
        })

        for row in data_iter:
            new_field = False

            for k in row.keys():
                if k not in fields:
                    fields.append(k)
                    rewrite = True
                    new_field = True

            if new_field:
                csv_writer = csv.DictWriter(
                    file_tmp,
                    fieldnames=fields,
                    delimiter=delimiter
                )

            csv_writer.writerow({
                k: _normalize_value(v)
                for k, v in row.items()
            })
            count += 1

            if shard_size and count >= shard_size:
                if shard_num:
                    print(
                        f"Wrote csv shard {shard_num} with {count} lines!"
                    )
                else:
                    print(f"Wrote csv shard file with {count} lines!")
                return file_tmp.name, True

    if shard_size:
        print(f"Wrote csv shard file with {count} lines!")
        return file_tmp.name, False

    if dest_path:
        if rewrite:
            with open(dest_path, 'w', encoding=encoding) as result_file:
                csv_writer = csv.DictWriter(
                    result_file,
                    fieldnames=fields,
                    delimiter=delimiter
                )
                csv_writer.writeheader()

                tmp_reader = csv_reader(
                    file_tmp.name,
                    fieldnames=fields,
                    delimiter=delimiter,
                    skip_lines=1,
                    encoding=encoding
                )
                csv_writer.writerows(tmp_reader)

            os.remove(file_tmp.name)

        else:
            shutil.move(file_tmp.name, dest_path)
    else:
        dest_path = file_tmp.name

    print(f"Wrote csv file {dest_path} with {count} lines!")

    return dest_path, False


def _normalize_value(v):
    if isinstance(v, (list, dict)):
        return json.dumps(v, ensure_ascii=False)
    return v


def load_csv_dict(csv_file: str,
                  id_field: str,
                  name_field: Optional[str] = None,
                  aggregate_common: bool = False,
                  **kwargs) -> Union[Dict[str, str], Dict[str, List[str]]]:
    """Read a `csv_file` and create a dict with the `id_field` as the key.

    Args:
        csv_file: File path to read the CSV file.
        id_field: Column name to be set as the dict resulting key.
        name_field (optional): Field to be set as the value of the dict. If the
        value is `None` all fields are returned as a dict.
        aggregate_common: If set to `True` and `name_field` is set, repeated
        values are added as a list. Default to `False`.
        **kwargs: All values in `kwargs` will be send to
        `libs.csv_util#csv_reader`.

    Returns:
        A dict which the key is the value of `id_field` and the value is the
        row data.

    Raises:
        FileNotFoundError: If the file does not exists in the path.
        KeyError: If the header `id_field` or `name_field` does not exists.

    Todo:
        * Support to `aggregate_common` without the param `name_field`.
    """

    csv_dict = {}

    for row in csv_reader(csv_file, **kwargs):
        field = _get_key_or_show_keys(row, id_field)

        # Get full line into id
        if not name_field:

            # Create id dict
            csv_dict[field] = dict(row)

        # Get reference into id
        else:
            value = _get_key_or_show_keys(row, name_field)

            # If multiple id_fields, values go into list
            if aggregate_common:
                if field in csv_dict:
                    csv_dict[field].append(value)
                else:
                    csv_dict[field] = [value]

            else:
                csv_dict[field] = value

    return csv_dict


def _get_key_or_show_keys(dict_data, key):
    try:
        assert key in dict_data
    except AssertionError:
        all_fields = ', '.join(f"\"{f}\"" for f in sorted(dict_data.keys()))
        raise KeyError(
            f"Field \"{key}\" not found! "
            f"But this file has the headers {all_fields}."
        ) from None

    return dict_data[key]