import functools

import pyflink.table.udf
from pyflink.table import DataTypes
from pyflink.table.udf import ScalarFunction, udf, TableFunction


class HashCode(ScalarFunction):
    def __init__(self):
        self.factor = 12

    def eval(self, s):
        return hash(s) * self.factor


hash_code = udf(HashCode(), result_type=DataTypes.BIGINT())


# 方式一：扩展基类 calarFunction
class Add(ScalarFunction):
    def eval(self, i, j):
        return i + j


add = udf(Add(), result_type=DataTypes.BIGINT())


# 方式二：普通 Python 函数
@udf(result_type=DataTypes.BIGINT())
def add2(i, j):
    return i + j


# 方式三：lambda 函数
add3 = udf(lambda i, j: i + j, result_type=DataTypes.BIGINT())


# 方式四：callable 函数
class CallableAdd(object):
    def __call__(self, i, j):
        return i + j


add4 = udf(CallableAdd(), result_type=DataTypes.BIGINT())


# 方式五：partial 函数
def partial_add(i, j, k):
    return i + j + k


add5 = udf(functools.partial(partial_add, k=1), result_type=DataTypes.BIGINT())
