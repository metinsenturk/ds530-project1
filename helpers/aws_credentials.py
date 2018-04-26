import csv


class aws_credentials:
    access_key_id = ''
    access_key_secret = ''

    def __self__(self, file_path):
        credentials = []

        with open(file_path, 'rb') as f:
            reader = csv.reader(f, delimiter=',')

            for row in reader:
                if reader[1, 3] and reader[1, 4]:
                    credentials.append()

        self.access_key_id = credentials[1]
        self.access_key_secret = credentials[2]
