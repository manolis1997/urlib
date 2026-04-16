from pyspark.sql import DataFrame
from typing import List, Optional
from pyspark.sql.functions import current_timestamp, lit, concat_ws, sha2
from pyspark.sql.types import DateType
from urlib.read_yaml.read_yaml import ReadYaml
from urlib.spark_session.spark_class import SparkClass
from delta.tables import DeltaTable

class ScdTypeTwo(SparkClass):
    def __init__(self, source_df : DataFrame, primary_keys: Optional[List[str]] = None, non_primary_keys: Optional[List[str]] = None, target_table_name: ReadYaml = None):
        self.source_df = source_df
        self.primary_keys = primary_keys
        self.non_primary_keys = non_primary_keys
        self.target_table_name = target_table_name
    
    @property
    def adding_scd_cols(self):
        source_df = (
            self.source_df
                .withColumn('key', concat_ws('||', *self.primary_keys))
                .withColumn('hash', sha2(concat_ws('||', *self.non_primary_keys), 256))
                .withColumn('start_date', current_timestamp())
                .withColumn('end_date', lit('9999-12-31').cast(DateType()))
                .withColumn('is_active', lit('Y'))
        )

        return source_df
    
    @property
    def __read_target_table(self):
        return self.spark.read.table(self.target_table_name)
    
    @property
    def __read_source_table(self):
        return self.adding_scd_cols
    
    @property
    def __fetch_inserts(self):
        inserts = (
            self.__read_source_table.alias('src')
            .join(self.__read_target_table.alias('trg').where("trg.is_active = 'Y'"), on='key', how='left')
            .where('trg.key is null')
        )

        inserts = inserts.selectExpr('src.*','null as mergeKey')

        return inserts


    @property
    def __fetch_updates(self):
        updates = (
            self.__read_source_table.alias('src')
            .join(self.__read_target_table.alias('trg').where("trg.is_active = 'Y'"), on='key', how='inner')
            .where('src.hash <> trg.hash')
        )

        updates_merge_key     = updates.selectExpr('src.*','key as mergeKey')
        updates_null_merge_key = updates.selectExpr('src.*','null as mergeKey')

        return updates_merge_key.unionByName(updates_null_merge_key)
    
    @property
    def __union_inserts_and_updates(self):
        return self.__fetch_inserts.unionByName(self.__fetch_updates)
    
    @property
    def debug_function(self):
        self.__read_target_table.show()
        self.__read_source_table.show()
        self.__union_inserts_and_updates.show()
    
    @property
    def merge_source_to_target(self):
        target_delta = DeltaTable.forName(self.spark, self.target_table_name)

        (
            target_delta.alias('trg')
            .merge(
                self.__union_inserts_and_updates.alias('src'), 
                condition = "trg.key = src.mergeKey and trg.is_active = 'Y'"
                )
                .whenMatchedUpdate(
                    condition = "trg.hash <> src.hash",
                    set = {
                        "is_active": lit('N'),
                        "end_date": lit(current_timestamp())
                    }
                )
                .whenNotMatchedInsert(
                    values = {
                    "key": "src.key",
                    "hash": "src.hash",
                    "start_date": "src.start_date",
                    "end_date": "src.end_date",
                    "is_active": "src.is_active",
                    **{col: f"src.{col}" for col in self.primary_keys + self.non_primary_keys}
                    }
                ).execute()
        )
