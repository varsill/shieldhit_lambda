import base64
import json
import os
from typing import List, Dict, Callable

def id(x):
    return x


def _read_file(file_name, transform):
    with open(file_name, "rb") as file:
        content = file.read()
        transformed = transform(content)
        encoded = base64.b64encode(transformed)
        encoded_str = encoded.decode("utf-8")
        return encoded_str


class Converters:
    """
    A module delivering functions to convert arbitrary files into dictionaries
    or JSON files (and another way around) so that to allow to transfer
    them via the network.
    """

    @staticmethod
    def files_to_map(
        input_filenames: List[str], transform: Callable[[bytes], bytes] = id
    ) -> Dict[str, str]:
        """
        A function that encodes a list of the files within a single dictionary.

        Args:
            input_filenames (List[str]): list of paths to the files out of which the result dictionary will be created
            transform (Callable[[bytes], bytes], optional): an inversible function that will be applied to the contents of the file -
                i.e. you can use a compression function to compress encoded binary. Remember, that if you want to received the exact content of the files, you
                need to apply the inverse of that function to the map_to_files function. If not provided, the transform won't be applied.

        Returns:
            Dict[str, str]: a result dictionary with keys being the files' names and the values being the encoded content of the files.
        """
        result = {}
        for filename in input_filenames:
            encoded = _read_file(filename, transform)
            _directory_path, just_file_name = os.path.split(filename)
            result[just_file_name] = encoded
        return result

    @staticmethod
    def files_to_json_file(
        input_filenames: List[str],
        output_path: str,
        transform: Callable[[bytes], bytes] = id,
    ) -> None:
        """
        A function that encodes a list of the files within a JSON file.

        Args:
            input_filenames (List[str]): list of paths to the files out of which the result dictionary will be created
            output_path (str): the path to the file where the JSON will be stored
            transform (Callable[[bytes], bytes], optional): an inversible function that will be applied to the contents of the file -
                i.e. you can use a compression function to compress encoded binary. Remember, that if you want to received the exact content of the files, you
                need to apply the inverse of that function to the map_to_files function.
        """
        result_map = Converters.files_to_map(input_filenames, transform)
        with open(output_path, "w") as output_file:
            output_file.write(json.dumps(result_map))

    @staticmethod
    def map_to_files(
        input_map: Dict[str, str],
        output_directory: str,
        transform: Callable[[bytes], bytes] = id,
        memfd: bool = False,
    ) -> None:
        """
        A function that takes a dictionary created with files_to_map() method and converts it back into the files out of which the
        dictionary has been created.

        Args:
            input_map (Dict[str, str]): a dictionary with keys being the files' names and the values being the encoded content of the files.
            output_directory (str): a path to the directory where the result files should be put
            transform (Callable[[bytes], bytes], optional): A function that will be applied to the contents of the encoded files before saving them in the filesystem.
                To restore the original content of the file, the function should be an inverse of the function passed as the 'transform' argument
                of the 'files_to_map' function. If not provided, the transform won't be applied.
            memfd (boolean, optional): If True, the file's content won't be stored in a filesystem and will persist within the memory. A symlink between the file name and the file descriptor will
                be created. If False, the file's content will be saved in the filesystem. Defaults to False.
        """
        for result_file_name in input_map.keys():
            content = input_map[result_file_name]
            decoded = base64.b64decode(content)
            transformed = transform(decoded)
            if not memfd:
                with open(
                    f"{output_directory}/{result_file_name}", "wb"
                ) as result_file:
                    result_file.write(transformed)
            else:
                pid = os.getpid()
                fd = os.memfd_create(result_file_name)
                memfd_path = f"/proc/{pid}/fd/{fd}"
                with open(memfd_path, "wb") as result_file:
                    result_file.write(transformed)
                os.symlink(memfd_path, f"{output_directory}/{result_file_name}")

    @staticmethod
    def json_file_to_files(
        input_path: str,
        output_directory: str,
        transform: Callable[[bytes], bytes] = id,
    ) -> None:
        """
        A function that converts the JSON with encoded files back into the original files.

        Args:
            input_path (str): a path to the file created with 'files_to_json_file' function.
            output_directory (str): a path to the directory where the result files should be put
            transform (Callable[[bytes], bytes], optional): A function that will be applied to the contents of the encoded files before saving them in the filesystem.
                To restore the original content of the file, the function should be an inverse of the function passed as the 'transform' argument
                of the 'files_to_map' function. If not provided, the transform won't be applied.
        """
        with open(input_path, "rb") as input:
            content = input.read().decode("utf-8")
            input_map = json.loads(content)
            Converters.map_to_files(input_map, output_directory, transform)
