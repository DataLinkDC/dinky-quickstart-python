from pyflink.common import Row
from pyflink.table import ListView, DataTypes
from pyflink.table.udf import udaf, udtaf, TableAggregateFunction, AggregateFunction


class WeightedAvg(AggregateFunction):

    def create_accumulator(self):
        # Row(sum, count)
        return Row(0, 0)

    def get_value(self, accumulator):
        if accumulator[1] == 0:
            return None
        else:
            return accumulator[0] / accumulator[1]

    def accumulate(self, accumulator, value, weight):
        accumulator[0] += value * weight
        accumulator[1] += weight

    def retract(self, accumulator, value, weight):
        accumulator[0] -= value * weight
        accumulator[1] -= weight

    def get_result_type(self):
        return DataTypes.BIGINT()

    def get_accumulator_type(self):
        return  DataTypes.ROW([
            # declare the first column of the accumulator as a string ListView.
            DataTypes.FIELD("f0", DataTypes.BIGINT()),
            DataTypes.FIELD("f1", DataTypes.BIGINT())])


# weighted_avg = udaf(WeightedAvg())
weighted_avg = udaf(WeightedAvg(), result_type=DataTypes.BIGINT())


class ListViewConcatAggregateFunction(AggregateFunction):

    def get_value(self, accumulator):
        # the ListView is iterable
        return accumulator[1].join(accumulator[0])

    def create_accumulator(self):
        return Row(ListView(), '')

    def accumulate(self, accumulator, *args):
        accumulator[1] = args[1]
        # the ListView support add, clear and iterate operations.
        accumulator[0].add(args[0])

    def get_accumulator_type(self):
        return DataTypes.ROW([
            # declare the first column of the accumulator as a string ListView.
            DataTypes.FIELD("f0", DataTypes.LIST_VIEW(DataTypes.STRING())),
            DataTypes.FIELD("f1", DataTypes.BIGINT())])

    def get_result_type(self):
        return DataTypes.STRING()


list_view_concat_aggregate_function = udaf(ListViewConcatAggregateFunction())


class Top2(TableAggregateFunction):

    def emit_value(self, accumulator):
        yield Row(accumulator[0])
        yield Row(accumulator[1])

    def create_accumulator(self):
        return [None, None]

    def accumulate(self, accumulator, row):
        if row[0] is not None:
            if accumulator[0] is None or row[0] > accumulator[0]:
                accumulator[1] = accumulator[0]
                accumulator[0] = row[0]
            elif accumulator[1] is None or row[0] > accumulator[1]:
                accumulator[1] = row[0]

    def get_accumulator_type(self):
        return 'ARRAY<BIGINT>'

    def get_result_type(self):
        return 'ROW<a BIGINT>'


# top2 = udtaf(Top2())
top2 = udtaf(Top2(), result_type=DataTypes.ROW([DataTypes.FIELD("a", DataTypes.BIGINT())]),
             accumulator_type=DataTypes.ARRAY(DataTypes.BIGINT()))


class ListViewConcatTableAggregateFunction(TableAggregateFunction):

    def emit_value(self, accumulator):
        result = accumulator[1].join(accumulator[0])
        yield Row(result)
        yield Row(result)

    def create_accumulator(self):
        return Row(ListView(), '')

    def accumulate(self, accumulator, *args):
        accumulator[1] = args[1]
        accumulator[0].add(args[0])

    def get_accumulator_type(self):
        return DataTypes.ROW([
            DataTypes.FIELD("f0", DataTypes.LIST_VIEW(DataTypes.STRING())),
            DataTypes.FIELD("f1", DataTypes.BIGINT())])

    def get_result_type(self):
        return DataTypes.ROW([DataTypes.FIELD("a", DataTypes.STRING())])


list_view_concat_table_aggregate_function = udtaf(ListViewConcatTableAggregateFunction())
