############################## IMPORT LIBRARIES ##############################

from functools import reduce

import inspect

import json;

from pyspark.sql import DataFrame
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructType,
    StructField,
    DateType
)
from pyspark.sql.functions import (
    array_min,
    array_max,
    col,
    collect_set,
    countDistinct,
    expr,
    first,
    lit,
    max,
    monotonically_increasing_id,
    round,
    when,
)
from pyspark.sql import Window

# For being able to run perl in a subprocess created by python
import subprocess

############################## VIEWING FUNCTIONS ##############################

def show_sorted(df, n = (10,)):
    df.select(sorted(df.columns)).show(truncate = False)

############################## JOINING FUNCTIONS ##############################

def concat_by_columns(df_list):
    join_id_df_list = list(map((lambda dataset: dataset.withColumn('join_id', monotonically_increasing_id())), df_list))
    final_df = reduce((lambda a, b: a.join(b, on = 'join_id')), join_id_df_list)
    return final_df.drop('join_id')

############################## WRITE FUNCTIONS ################################

# Method to write a pyspark df to disk, by using `write_to_<format>` as a method
# on the pysapark df to be written

# Use case: Helps in reducing boilerplate code and maintains method chaining

def write_to_parquet(self, dest_file_name, parent_data_folder = parent_data_folder):
    self.write.parquet(parent_data_folder + dest_file_name, mode = 'overwrite')
    return self

def write_to_csv(self, dest_file_name, parent_data_folder = parent_data_folder):
    self.write.csv(parent_data_folder + dest_file_name, mode = 'overwrite')
    return self

############################## READ FUNCTIONS ################################

# Methods to read <format> files into a pyspark df

# Use case: Reduces boilerplate code like `spark.read.format...`

def read_parquet(source_file_name, parent_data_folder = parent_data_folder):
    df = spark.read.format('parquet').load(parent_data_folder + source_file_name)
    return df

def read_csv(source_file_name, parent_data_folder = parent_data_folder):
    df = spark.read.format('csv').options(header = True, inferSchema = True).load(parent_data_folder + source_file_name)
    return df

############################## READ FUNCTIONS ################################

# def withColumns(df: DataFrame, col_definitions: dict) -> DataFrame:
def withColumnsOrdered(self: DataFrame, col_definitions: dict) -> DataFrame:
    # Create lst to contain the names of all the new columns being calculated
    dep_cols_lst = col_definitions.keys()
    # Read the column calculation expression into a lst. 1 expression per column calculation
    expr_lst = '\n'.join([str(expr) for expr in col_definitions.values()])
    # Create lst, with each element being a lst of all the columns from which the new column is getting calculated
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
    # Loop over the original dict
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

def crossTab1GroupCol(
    self,
    value_col,
    group_col,
):
    freq_df = (
        self
        .select(
            col(value_col),
            col(group_col),
        )
        .withColumns({
            'present': lit(1)
        })
        .withColumns({
            group_col: col(group_col).cast('string')
        })
        .fillna(
            'Null',
            subset = group_col
        )
        .groupBy(value_col)
        .agg(collect_set(group_col).alias('groups'))
        .groupBy('groups')
        .agg(countDistinct(value_col).alias('count'))
        .withColumns({
            'col_1': array_min(col('groups')),
            'col_2': array_max(col('groups')),
        })
        .groupBy('col_1')
        .pivot('col_2')
        .agg(first('count'))
    )
    freq_df = (
        freq_df
        .fillna(
            0,
            subset = (
                freq_df
                .drop('col_1')
                .columns
            )
        )
        .withColumnRenamed(
            'col_1',
            group_col
        )
    )
    return freq_df

def groupedLag(
    laggedCol,
    partitionByCols,
    orderByCols,
    lagGroupCols
):
    for cols in [
        partitionByCols,
        orderByCols,
        lagGroupCols
    ]:
        partitionByCols = [partitionByCols] if not isinstance(partitionByCols, list) else partitionByCols
        orderByCols = [orderByCols] if not isinstance(orderByCols, list) else orderByCols
        lagGroupCols = [lagGroupCols] if not isinstance(lagGroupCols, list) else lagGroupCols
    normal_lag_col = (
        lag(laggedCol)
        .over(
            Window
            .partitionBy(partitionByCols)
            .orderBy(orderByCols)
        )
    )
    grouped_lag_expr = (
        first(normal_lag_col)
        .over(Window.partitionBy(lagGroupCols))
    )
    return grouped_lag_expr

############### DIRECT COUNTDISTINCT METHOD ################################

def countDistinctAgg(self): 
    custom_count_func = size(collect_set(countCol))
    return custom_count_func

############################## MONKEY PATCHING #############################

# Add the above created methods to pyspark's DataFrame class

# Use case: This helps in ensuring that I can use them as methods on any pyspark df

# Caveat: This works only because pysaprk doesn't import the DataFrame class internally
#         to create DataFrame instances, and instead uses the monkey-patched class to
#         create the new instances

for func in [
    show_sorted,
    concat_by_columns,
    write_to_parquet,
    write_to_csv,
    read_parquet,
    read_csv,
    withColumnsOrdered,
    crossTab1GroupCol 
]:
    # DataFrame.func = func
    setattr(
        DataFrame,
        func.__name__,
        func
    )
