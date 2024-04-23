class ReadingError(Exception):
    pass


class CsvReadingError(ReadingError):
    pass


class DeltaReadingError(ReadingError):
    pass


class DeltaWritingError(Exception):
    pass


class DeltaUpsertError(Exception):
    pass
