from typing import Literal

# Some of minio metadata attributes returned when searching for objects metadata
# in calls to methods like stat_object()
USER_META_VERSION_ATT = 'x-amz-meta-version'
OBJECT_META_VERSION_ATT = 'x-amz-version-id'
OBJECT_META_TAGCOUNT_ATT = 'x-amz-tagging-count'
OBJECT_META_LAST_MODIFIED_ATT = 'Last-Modified'
OBJECT_META_DATE_ATT = 'Date'
OBJECT_CONTENT_TYPE_ATT = 'Content-Type'
OBJECT_META_CONTENT_LENGTH_ATT = 'Content-Length'
OBJECT_META_ETAG_ATT = 'ETag'

MAX_TAG_DESCRIPTION_LEN = 255

# Minio object content types constants for upload on bucket    
CsvLike = Literal["application/csv", "text/csv"]
Text = "application/octet-stream"
Spreadsheet = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
MacroEnabledSpreadSheet = "application/vnd.ms-excel.sheet.macroenabled.12"
Zipped= "application/x-zip-compressed"
Json = "application/json"
