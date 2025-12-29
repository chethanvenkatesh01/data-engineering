class DaySplit:
    def __init__(self, project, dataset, modelling_level, day_split_level):
        from airflow.models import Variable
        import urllib
        self.PGUSER = Variable.get("PGXUSER")
        self.PGPASSWORD = Variable.get("PGXPASSWORD")
        self.PGHOST = Variable.get("PGXHOST")
        self.PGPORT = Variable.get("PGXPORT")
        self.PGDATABASE = Variable.get("PGXDATABASE")
        self.pg_schema = Variable.get("PGXSCHEMA")
        self.project = project
        self.dataset = dataset
        self.modelling_level = modelling_level  # level at which day split is calculated
        self.day_split_level = day_split_level  # level at which the ada serve expects the final table to be
        self.conn_string = (
            "postgresql://"
            + self.PGUSER
            + ":"
            + urllib.parse.quote(self.PGPASSWORD)
            + "@"
            + self.PGHOST
            + ":"
            + "5432"
            + "/"
            + self.PGDATABASE
        )

    def get_events(self):

        import psycopg2
        import pandas as pd
        """
        finding all the events
        """
        query_to_find_event_mapping_table = f"""
        select generic_mapping_table from {self.pg_schema}.generic_master_mapping where destination_table = 'event_master'
        """
        with psycopg2.connect(host=self.PGHOST,
                 database=self.PGDATABASE,
                 port=self.PGPORT,
                 user=self.PGUSER ,
                 password=self.PGPASSWORD) as conn:
            event_schema_mapping = (
                pd.read_sql(query_to_find_event_mapping_table, conn)
                .iloc[:, 0]
                .to_list()[0]
            )

        # query to find the events present in event generic schema mapping
        query_1 = f"""
        select generic_column_name from {self.pg_schema}.{event_schema_mapping} where generic_column_name <> 'date'
        """

        # query to find extra events from FMD if present
        query_2 = f"""
        select column_name  from {self.pg_schema}.feature_metadata fm  where  entity= 'ada_serve_day_split_creation'
        """

        with psycopg2.connect(host=self.PGHOST,
                 database=self.PGDATABASE,
                 port=self.PGPORT,
                 user=self.PGUSER ,
                 password=self.PGPASSWORD) as conn:
            events = pd.read_sql(query_1, conn).iloc[:, 0].to_list()
            extra_events = pd.read_sql(query_2, conn).iloc[:, 0].to_list()
            events += extra_events
        return events

    def find_end_year_week(self):
        # retreive the yearweek for last 1 year from master table
        from datetime import datetime, timedelta
        from google.cloud import bigquery
        from airflow.models import Variable
        current_date = datetime.now()
        one_year_ago = str((current_date - timedelta(days=365)).date())
        client = bigquery.Client(project=self.project, location=Variable.get("bigquery_region", default_var="US"))
        job = client.query(
            f"select max(fiscal_year_week) from {self.dataset}.master_input_imputed where date > {one_year_ago}",
        )
        client.close()
        return job.to_dataframe().iloc[0, 0]

    def create_group_by_string(self, len_hierarchies, len_additional_cols):
        string_group_by = ""
        for i in range(len_hierarchies + len_additional_cols):
            string_group_by += f"{i+1},"
        string_group_by = string_group_by[:-1]
        return string_group_by

    def find_hierarchies(self, level):
        # finding the hierarchy names less than or equal to modelling level
        import psycopg2
        import pandas as pd
        query_to_find_product_mapping_table = f"""
        select generic_mapping_table from {self.pg_schema}.generic_master_mapping where destination_table = 'product_master'
        """
        with psycopg2.connect(host=self.PGHOST,
                 database=self.PGDATABASE,
                 port=self.PGPORT,
                 user=self.PGUSER ,
                 password=self.PGPASSWORD) as conn:
            product_schema_mapping = (
                pd.read_sql(query_to_find_product_mapping_table, conn)
                .iloc[:, 0]
                .to_list()[0]
            )

        query_to_find_hierarchy = f"""
        select generic_column_name from {self.pg_schema}.{product_schema_mapping} where
        hierarchy_level<=(select hierarchy_level from {self.pg_schema}.{product_schema_mapping} where
        generic_column_name='{level}') order by hierarchy_level asc
        """
        with psycopg2.connect(host=self.PGHOST,
                 database=self.PGDATABASE,
                 port=self.PGPORT,
                 user=self.PGUSER ,
                 password=self.PGPASSWORD) as conn:
            hierarchies = (
                pd.read_sql(query_to_find_hierarchy, conn).iloc[:, 0].to_list()
            )
        return hierarchies

    def create_temp_table_on_bq(self):
        import psycopg2
        import pandas as pd
        from tasks.utils import year_week_to_date
        from airflow.models import Variable
        """
        day split is calculated at the modelling level and the table is saved as day_split_temporary in bq
        """

        hierarchies = self.find_hierarchies(self.modelling_level)
        num_hierarchies = len(hierarchies)
        string_hierarchies = ""
        for hierarchy in hierarchies:
            string_hierarchies += f"{hierarchy}, "
        string_hierarchies = string_hierarchies[:-2]


        end_year_week = self.find_end_year_week()

        # find the quantity column name
        query_to_find_qty_column_name = f"""
        select column_name  from {self.pg_schema}.feature_metadata where entity='impute_features' and feature_type = 'qty'
        """
        with psycopg2.connect(host=self.PGHOST,
                 database=self.PGDATABASE,
                 port=self.PGPORT,
                 user=self.PGUSER ,
                 password=self.PGPASSWORD) as conn:
            qty_column_name = (
                pd.read_sql(query_to_find_qty_column_name, conn).iloc[:, 0].to_list()[0]
                + "_imputed"
            )

        # day split logic

        # query to extract the day_of_week, quantity_imputed and total_quantity of the modelling level
        # for the latest one year
        end_year_date_lower_threshold = year_week_to_date(end_year_week-100,agg_func='min')
        end_year_date_upper_threshold = year_week_to_date(end_year_week,agg_func='max')
        query_1 = f"""
        with base_table as (
        select channel, {string_hierarchies}, EXTRACT(DAYOFWEEK from date) as dayofweek, {qty_column_name}, sum({qty_column_name}) over(partition by channel, {string_hierarchies}) as total_quantity
        from `{self.project}.{self.dataset}.master_input_imputed` a
        where {qty_column_name} > 0 AND
        date between '{end_year_date_lower_threshold}' and '{end_year_date_upper_threshold}'
        """

        # query which finds all the events and filter out the non-event days
        query_2 = """"""
        events = self.get_events()
        for event in events:
            query_2 += f"""
            and ({event} in (0) or {event} is NULL) 
            """
        query_2 += """),"""

        on_condition = ""
        for hierarchy in hierarchies:
            on_condition += f" AND a.{hierarchy}=b.{hierarchy}"

        # calculation of dayofweek_quantity along with imputation of null values
        query_3 = f"""
          base_table_2 AS (
            SELECT
                a.*,
                b.dayofweek_quantity,
                COALESCE(b.ratio,SAFE_DIVIDE(SUM(dayofweek_quantity) OVER (PARTITION BY a.channel, a.dayofweek),SUM(dayofweek_quantity) OVER (PARTITION BY a.channel) ) ) AS ratio,
                b.total_quantity
            FROM (
                SELECT
                *
                FROM (
                SELECT
                    DISTINCT channel
                FROM
                    `{self.project}.{self.dataset}.master_input_imputed`
                WHERE
                    channel IS NOT NULL ) a
                INNER JOIN (
                SELECT
                    DISTINCT {string_hierarchies}
                FROM
                    `{self.project}.{self.dataset}.product_master`
                WHERE
                    {self.modelling_level} IS NOT NULL ) b
                ON
                1=1
                INNER JOIN (
                SELECT
                    DISTINCT EXTRACT(dayofweek
                    FROM
                    date) AS dayofweek
                FROM
                    `{self.project}.{self.dataset}.fiscal_date_mapping` ) c
                ON
                1=1 ) a
            LEFT JOIN (
                SELECT
                channel,
                {string_hierarchies},
                dayofweek,
                dayofweek_quantity,
                total_quantity,
                SAFE_DIVIDE(dayofweek_quantity,total_quantity) AS ratio
                FROM (
                SELECT
                    channel,
                    {string_hierarchies},
                    total_quantity,
                    dayofweek,
                    SUM({qty_column_name}) AS dayofweek_quantity
                FROM
                    base_table
                GROUP BY {self.create_group_by_string(num_hierarchies, 3)})) b
            ON
                a.channel=b.channel
                {on_condition}
                AND a.dayofweek=b.dayofweek),
                    """

        # total_contribution calculation
        query_4 = f"""
            norm_table AS (
            SELECT
                channel,
                {string_hierarchies},
                SUM(ratio) AS total_contri
            FROM
                base_table_2
            GROUP BY {self.create_group_by_string(num_hierarchies, 1)})
        """

        # calculation of ratio
        query_5 = f"""
            SELECT
            *EXCEPT(ratio),
            ratio/total_contri AS ratio
            FROM
            base_table_2
            LEFT JOIN
            norm_table
            USING
            (channel, {string_hierarchies})
        """

        query = query_1 + query_2 + query_3 + query_4 + query_5

        Variable.set("day_split_temp_query", query)

    def create_day_split_on_bq(self):
        import re
        from airflow.models import Variable
        """
        day_split_temporary table from bq is mapped to the respective hierarchy level (day_split_level) expected by ada serve
        """

        # retreiving the level from hierarchy_levels
        comp_re = re.compile("l(\d+)_name")
        day_split_hierarchy_level = int(comp_re.findall(self.day_split_level)[0]) + 1

        hierarchies = self.find_hierarchies(self.modelling_level)
        string_hierarchies = ""
        for hierarchy in hierarchies:
            string_hierarchies += f"{hierarchy}, "
        string_hierarchies = string_hierarchies[:-2]

        day_split_hierarchies = self.find_hierarchies(self.day_split_level)
        ds_string_hierarchies = ""
        for hierarchy in day_split_hierarchies:
            ds_string_hierarchies += f"{hierarchy}, "
        ds_string_hierarchies = ds_string_hierarchies[:-2]

        query = f"""
        with table1 as (
        select * from {self.project}.{self.dataset}.day_split_temporary
        inner join (select hierarchy_code, {ds_string_hierarchies} 
        from {self.project}.{self.dataset}.product_hierarchies_filter_flattened where level={day_split_hierarchy_level} and active=True)
        using({string_hierarchies}))

        select hierarchy_code,channel,dayofweek,dayofweek_quantity,total_quantity,ratio from table1
        """

        Variable.set("day_split_query", query)

    # def create_day_split_on_bq(self):
    #     """
    #     day_split_temporary table from bq is mapped to the respective hierarchy level (day_split_level) expected by ada serve
    #     """

    #     query_1 = f"""select * from {self.project}.{self.dataset}.day_split_temporary"""
    #     temp_table = pd.read_gbq(query_1, project_id=self.project, location=Variable.get("bigquery_region", default_var="US"))
    #     temp_table = temp_table.dropna()

    #     # retreiving the level from hierarchy_levels
    #     comp_re = re.compile("l(\d+)_name")
    #     modelling_hierarchy_level = int(comp_re.findall(self.modelling_level)[0]) + 1
    #     day_split_hierarchy_level = int(comp_re.findall(self.day_split_level)[0]) + 1

    #     query_to_flatten_hierarchies = f"""
    #     select * from {self.pg_schema}.product_hierarchies_filter_flattened where active=true and level={day_split_hierarchy_level}
    #     """
    #     with psycopg2.connect(host=self.PGHOST,
    #              database=self.PGDATABASE,
    #              port=self.PGPORT,
    #              user=self.PGUSER ,
    #              password=self.PGPASSWORD) as conn:
    #         hierarchies_df = pd.read_sql(query_to_flatten_hierarchies, conn)

    #     # for each row in the day_split_temporary, all the curresponding 'day_split_level' combinations are calculated from product_hierarchies_filter_flattened
    #     # and the day split values are copied to those combinations
    #     day_split = pd.DataFrame()

    #     for index, row in temp_table.iterrows():
    #         temp_df = hierarchies_df.copy()
    #         # retreiving rows from product_hierarchies_filter_flattened which currespond to the current row
    #         for i in range(modelling_hierarchy_level):
    #             filter__ = temp_df[f"l{i}_name"].isin([row[f"l{i}_name"]])
    #             temp_df = temp_df[filter__]

    #         temp_df["channel"] = row["channel"]
    #         temp_df["dayofweek"] = row["dayofweek"]
    #         temp_df["dayofweek_quantity"] = row["dayofweek_quantity"]
    #         temp_df["total_quantity"] = row["total_quantity"]
    #         temp_df["total_contri"] = row["total_contri"]
    #         temp_df["ratio"] = row["ratio"]

    #         day_split = pd.concat([day_split, temp_df])

    #     day_split = day_split[
    #         [
    #             "hierarchy_code",
    #             "channel",
    #             "dayofweek",
    #             "dayofweek_quantity",
    #             "total_quantity",
    #             "total_contri",
    #             "ratio",
    #         ]
    #     ]

    #     pandas_gbq.to_gbq(
    #         day_split,
    #         f"{self.dataset}.day_split_table",
    #         project_id=self.project,
    #         if_exists="replace",
    #     )
