class BarecatError(Exception):
    def __init__(self, message: str):
        super().__init__(message)


class FileExistsBarecatError(BarecatError):
    def __init__(self, path: str):
        super().__init__(f'File already exists: {path}')


class FileNotFoundBarecatError(BarecatError):
    def __init__(self, path: str):
        super().__init__(f'File not found: {path}')


class DirectoryNotEmptyBarecatError(BarecatError):
    def __init__(self, path: str):
        super().__init__(f'Directory not empty: {path}')


class IsADirectoryBarecatError(BarecatError):
    def __init__(self, path: str):
        super().__init__(f'Is a directory: {path}')


class BarecatIntegrityError(BarecatError):
    def __init__(self, message: str):
        super().__init__(message)


class NotEnoughSpaceBarecatError(BarecatError):
    def __init__(self, message: str):
        super().__init__(message)
