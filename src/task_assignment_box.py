import sqlalchemy


class Database:

    def __init__(self, user, password, db, host, port):
        self.user = user
        self.password = password
        self.db = db
        self.host = host
        self.port = port

    def connect(self):
        '''Returns a connection and a metadata object'''
        # connect with the help of the PostgreSQL URL
        url = 'postgresql://{}:{}@{}:{}/{}'
        url = url.format(self.user, self.password, self.host, self.port, self.db)

        # connection object
        con = sqlalchemy.create_engine(url, client_encoding='utf8')

        # bind the connection to MetaData()
        meta = sqlalchemy.MetaData(bind=con, reflect=True)

        return con, meta


class TaskAssignmentBaseline:

    def __init__(self, con, job_id, worker_id, max_items):
        self.con = con
        self.job_id = job_id
        self.worker_id = worker_id
        self.max_items = max_items

    def get_tasks(self):
        items = [1, 2, 3]
        criteria = [1]

        items_total = 100
        filter_list = [1, 2, 3]
        items_tolabel = 10

        for filter in filter_list:
            if items_tolabel == items_total:
                continue
            if items_tolabel >= self.max_items:
                return items, criteria
            else:
                return items, criteria

        return None, None
