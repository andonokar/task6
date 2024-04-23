class ReadingError(Exception):
    pass


class CsvReadingError(ReadingError):
    pass


class DeltaReadingError(ReadingError):
    pass