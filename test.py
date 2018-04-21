import boto3

a_c_c_e_s_s__k_e_y = 'AKIAIBBRXQ4FUMMEHIEQ'
s_e_c_r_e_t__k_e_y = 'LGlXz4iUWjHYsl7zI9uWVNXDy0FYkT92tfgTKTy4'
r_e_g_i_o_n__n_a_m_e = 'us-east-1'

dynamodb = boto3.client('dynamodb',
      endpoint_url='http://localhost:8000/')

response = dynamodb.list_tables(
    ExclusiveStartTableName='string',
    Limit=10
)
items = response['TableNames']
print(items)

response = dynamodb.create_table()

"""
# inserting into a table
rq = requests.request('GET', 'https://randomuser.me/api/')
user_data = rq.json()['results']

item = {
    'gender': {
        'S': user_data[0]['gender'],
    },
    'cell': {
        'S': user_data[0]['cell'],
    },
    'phone': {
        'S': user_data[0]['phone'],
    },
    'registered': {
        'S': user_data[0]['registered'],
    }
}

user_inserted = dydb.insert_item('users', item)
print(user_inserted)
print(user_inserted)
"""