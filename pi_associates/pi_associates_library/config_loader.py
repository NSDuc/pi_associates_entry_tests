import dotenv
import os


class ConfigLoader:
    def load_config(self, **kwargs):
        raise NotImplementedError


class DotenvConfigLoader(ConfigLoader):
    def load_config(self, filename):
        filepath = dotenv.find_dotenv(filename)
        return dotenv.dotenv_values(filepath)


class EnvironConfigLoader(ConfigLoader):
    def load_config(self):
        return os.environ.copy()
