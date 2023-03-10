from abc import ABC, abstractmethod


class Reducer(ABC):
    """
    An interface defining modules that serve as Reducer

    A Redeucer is a module that gets the Mappers output - .bdo files with generated samples, and aggregates
    that result with pymchelper.
    """

    @abstractmethod
    def __init__(self, input_files_dir: str, output_dir: str, operation: str = "image"):
        """
        Module's constructor.

        Args:
            input_files_dir (str): the path to the directory where the .bdo files are stored
            output_dir (str): the path to the directory where the result files should be stored
            operation (str, optional): An pymchelper aggregating operation that should be performed. For information about the available
              operations, take a look [here](https://pymchelper.readthedocs.io/en/stable/user_guide.html#available-converters). Defaults to "image".
        """
        pass

    @abstractmethod
    def execute(self):
        """
        A function that performs the samples aggregation. At the end of that function execution
        all the result files should be available in the desired location.
        """
