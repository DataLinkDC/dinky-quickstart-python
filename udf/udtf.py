# 方式一：生成器函数
from pyflink.table import DataTypes
from pyflink.table.udf import udtf, TableFunction


class Split(TableFunction):
    def eval(self, string):
        for s in string.split(" "):
            yield s, len(s)


split = udtf(Split(), result_types=[DataTypes.STRING(), DataTypes.INT()])


@udtf(result_types=[DataTypes.BIGINT()])
def generator_func(x):
    yield 1
    yield 2


# 方式二：返回迭代器
@udtf(result_types=[DataTypes.BIGINT()])
def iterator_func(x):
    return range(5)


# 方式三：返回可迭代子类
@udtf(result_types=[DataTypes.BIGINT()])
def iterable_func(x):
    result = [1, 2, 3]
    return result
