from abc import ABC, abstractmethod


class Mapper(ABC):
    """
    An interface defining modules that serve as Mappers.

    A Mapper is a module that generates the desired number of shieldhit's samples based on the
    provided input .dat files, and saves them as .bdo binary files. The process of samples generation
    need to be split among the desired number of workers.
    """

    @abstractmethod
    def __init__(
        self,
        how_many_samples: int,
        how_many_workers: int,
        input_files_dir: str,
        output_dir: str,
    ):
        """
        Module's constructor.

        Args:
            how_many_samples (int): Number of samples that should be generated
            how_many_workers (int): Number of workers that should be used to generate samples
            input_files_dir (str): location of the input configuration files (.dat files)
            output_dir (str): directory where the result .bdo files will be written to
        """
        pass

    @abstractmethod
    def execute(self) -> None:
        """
        A function that performs the samples generation. At the end of that function execution
        all the result .bdo files should be available in the desired location.
        """
        pass
