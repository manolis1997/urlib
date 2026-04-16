from pyspark.sql import functions as F
from utils.spark_session.spark_class import SparkClass

class SilverTransformation(SparkClass):
    def __init__(self, source_df, name, yaml_file):
        self.source_df = source_df
        self.name = name
        self.yaml_file = yaml_file

    def apply_transformations(self):
        columns_config = self.yaml_file['columns']

        select_exprs = []
        for col in columns_config:
            source_col = col['source']
            target_col = col['target']
            
            if 'transformation' in col:
                # Apply transformation
                # Use expr() to evaluate SQL-like expressions
                expr_str = col['transformation']
                select_exprs.append(F.expr(expr_str).alias(target_col))
            else:
                # No transformation, just rename if needed
                select_exprs.append(F.col(source_col).alias(target_col))

        # Create silver DataFrame
        df_silver = self.source_df.select(*select_exprs)
        
        return df_silver