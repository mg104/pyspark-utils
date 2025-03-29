############################## IMPORT LIBRARIES ##############################

# Typehints
from pyspark.sql import DataFrame
from pyspark.sql.column import Column
from typing import Dict

from pyspark.sql.functions import (
    col,
)

# Data interchange between python & perl
import json

# Running perl process
import subprocess

def withColumnsOrdered(
        self: DataFrame, 
        col_definitions: Dict[str, Column]
) -> DataFrame:
    """   
    Pyspark df method to calculate new columns in the same .withColumnsOrdered method call on pyspark df, even if
    the new columns might be interdependent on each other.

    This method divides single df.withColumnsOrdered({'col1': lit(1), 'col2': lit(col1)}) call into multiple 
    df.withColumns({'col1': lit(1)}).withColumns({'col2': lit(col1)}) calls (ordered according to the columns' interdependencies), 
    thereby eliminating the need to explicitly write these multiple calls ourselves.

    Args:
        self (DataFrame): Pyspark df on which new columns are being calculated
        col_definitions (Dict[str, Column]): New columns & their calculations
    
    Returns:
        self (DataFrame): Pyspark df containing newly calculated columns

    Usage:
        >>> from pyspark.sql import SparkSession
        >>> spark = SparkSession.builder.getOrCreate()
        >>> schema = StructType([
                StructField('col0', IntegerType(), True),
            ])
        >>> data = [
                [1, ],
                [1, ],
            ]
        >>> df = spark.createDataFrame(schema, data)
        >>> df = df.withColumnsOrdered({'col1': lit(1), 'col2': lit(col('col1'))})
    
    Notes: 
        This method doesn't yet take care of the following cases:
            1. Parsing 'higher-order' column-calculations where the independent column names aren't mentioned directly
                in the calculation.
        It currently handles calculations like:
            1. Non-window functions: lit, addition, subtraction, etc.
            2. Window functions
        This method also assumes that you have perl installed in your system.
    """

    # Convert original {col: calc} into text parseable by perl
    dep_cols_lst = col_definitions.keys()
    expr_lst = '\n'.join([str(expr) for expr in col_definitions.values()])

    # Strip out non-colnames from the text and get only independent colnames
    # E.g.: {'col1': sum(lit(1)).over(Window.partitionBy('col2'))} => 'col2'
    perl_code = r"""
        use JSON;
        my @overall_arr;
        while (<>) {
            my @indep_col_arr;
            while (/(\b\w+\b)(?!\()/g) {
                push @indep_col_arr, $1;
            }
            push @overall_arr, \@indep_col_arr;
        }
        $json_output = encode_json(\@overall_arr);
        print "$json_output";
     """
    result = subprocess.run(
        [
            "perl",
            "-e",
            perl_code
        ],
        input = expr_lst,
        capture_output = True,
        text = True
    )
    indep_cols_lst = json.loads(result.stdout)
    dep_indep_dict = dict(zip(dep_cols_lst, indep_cols_lst))

    # Iteratively find the columns that are not dependent on any other column for their calculations
    # and put them into a separate .withColumns(...) call
    while dep_indep_dict:
        prev_dep_cols_lst = []
        included_cols = []
        excluded_cols = []
        for dep_col, indep_col_lst in dep_indep_dict.copy().items():
            # 1. Find the keys which have values that are not same as any of the previous keys
            if bool(set(indep_col_lst) & set(prev_dep_cols_lst)):
                excluded_cols.append(dep_col)
            else:
                included_cols.append(dep_col)
                del dep_indep_dict[dep_col]
            prev_dep_cols_lst.append(dep_col)
        included_expr = {
            col: expr
            for col, expr in col_definitions.items()
            if col in included_cols
        }
        self = self.withColumns(included_expr)

    return self

############################## MONKEY PATCHING #############################

# Add the above created methods to pyspark's DataFrame clas. This helps in ensuring that I can 
# use them as methods on any pyspark df

for func in [
    withColumnsOrdered,
]:
    # DataFrame.func = func
    setattr(
        DataFrame,
        func.__name__,
        func
    )
