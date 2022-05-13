from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

from src.utils.common_string_parser import get_bucket_name


@udf(StringType())
def udf_get_bucket_name(path):
    return get_bucket_name(path)
