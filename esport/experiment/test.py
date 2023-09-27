# #
# data_to_insert = [{"A": "pertama","B": "kedua","C" : "ketiga"},
#                     {"A": "pertamax","B": "keduax","C" : "ketigax"},
#                   ]
#
# table_name = 'table_name'
# _list = []
# for i in data_to_insert:
#     column_names = ', '.join(i.keys())
#     placeholders = ', '.join(['%s'] * len(i))
#
# # Create the SQL query
#     query = f"""
#         INSERT INTO {table_name} ({column_names})
#         VALUES ({placeholders});
#     """
#     values_tuple = tuple(map(lambda key: i[key], i.keys()))
#     _list.append(values_tuple)
# print(_list)
#
#
# # data_dict = [{
# #     "A": "value_A",
# #     "B": "value_B",
# #     "C": "value_C"
# #         },
# #     {"A": "value_AX",
# #     "B": "value_BX",
# #     "C": "value_CX"}
# # ]
# #
# # for i in
# # # Using a lambda function to extract values and create a tuple
# # values_tuple = tuple(map(lambda key: data_dict[key], data_dict.keys()))
# #
# # # Print the resulting values tuple
# # print(values_tuple)
data = [{
    "A": "value_A",
    "B": "value_B",
    "C": "value_C"
        },
    {"A": "value_AX",
    "B": "value_BX",
    "C": "value_CX"}
]
# print(list(row.keys() for row in data))
dict = {
    "A": "value_A",
    "B": "value_B",
    "C": "value_C"
        }
# print(data.keys())

print(dict["A"])

values = [tuple(row[key] for key in row.keys()) for row in data]
#
print(values)
# for row in data :
#     values_lambda = tuple(map(lambda key: row[key], row.keys()))
#     print(values_lambda)

