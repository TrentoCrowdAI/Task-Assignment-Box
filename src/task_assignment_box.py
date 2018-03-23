import sqlalchemy
import pandas as pd


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
        sql_filter_list = '''
                        select c.* from job j 
                        join project p on j.project_id = p.id
                        join criterion c on c.project_id = p.id where j.id = {job_id};
                    '''.format(job_id=self.job_id)
        filter_list = pd.read_sql(sql_filter_list, self.con)['id'].values

        for filter_id in filter_list:
            sql_items_tolabel = '''
                        select i.id, coalesce(item_votes.votes, 0) as votes 
                        from (select item_id, votes from 
                          (select t.item_id, count(t.*) as votes from task t
                            where t.job_id = {job_id}
                              and t.data @> '{{"criteria" : [{{"id": "{filter_id}"}}]}}'
                              and t.data ->> 'answered' = 'true'
                            group by t.item_id
                          ) v 
                          where votes < {max_votes}
                                and item_id not in (
                                  select t.item_id from task t
                                  where t.job_id = {job_id}
                                        and t.worker_id = {worker_id}
                                        and t.data @> '{{"criteria" : [{{"id": "{filter_id}"}}]}}'
                                        and t.data ->> 'answered' = 'true'
                                )
                        ) item_votes right join item i on i.id = item_votes.item_id
                        where i.id not in (
                          select t.item_id from task t
                            where t.job_id = {job_id}
                              and t.worker_id = {worker_id}
                              and t.data @> '{{"criteria" : [{{"id": "{filter_id}"}}]}}'
                              and t.data ->> 'answered' = 'true'
                        );
                    '''.format(filter_id=filter_id, worker_id=self.worker_id, job_id=self.job_id, max_votes=10)

            items_tolabel = pd.read_sql(sql_items_tolabel, self.con)['id'].values
            items_tolabel_num = len(items_tolabel)

            if items_tolabel_num == 0:
                continue
            if items_tolabel_num >= self.max_items:
                return [int(i) for i in items_tolabel[:self.max_items]], [int(filter_id)]
            else:
                return [int(i) for i in items_tolabel], [int(filter_id)]

        return None, None
