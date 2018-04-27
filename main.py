import helpers.psql as p
import helpers.s3 as s
import helpers.redshift as r


def main():
    s3 = s.S3()
    s3.bucket_name = 'machin-ds530'

    redshift = r.Redshift()
    zagi = p.PSQL('zagi', 'localhost')

    zagi_table_names = ['category', 'customer', 'product', 'region',
                        'salestransaction', 'soldvia', 'store', 'vendor']

    for table_name in zagi_table_names:
        # write data to csv files
        zagi.table_to_csv(table_name)

        # s3 upload
        file = open('{}.csv'.format(table_name), 'rb')
        s3.put_object(file, 'zagi/{}.csv'.format(table_name))

    cluster = redshift.create_cluster('machindw', 'machinroot', '367Rabbit')

    machindw = p.PSQL('dev', cluster['Endpoint']['Address'], '5439', 'machinroot', '367Rabbit')
    machindw.copy(zagi_table_names[0], 'zagi/{}.csv'.format('category'))


def junk():
    s3 = s.S3()
    homeaway = p.PSQL('homeaway', 'localhost')
    homeaway_table_names = ['apartment', 'building', 'cleaning', 'corpclient',
                            'inspecting', 'inspector', 'manager', 'managerphone', 'staffmember']

    for table_name in homeaway_table_names:
        # write data to csv files
        homeaway.table_to_csv(table_name)

        # s3 upload
        file = open('{}'.format(table_name), 'rb')
        s3.put_object(file, 'homeaway/{}.csv'.format(table_name))


if __name__ == '__main__':
    main()
