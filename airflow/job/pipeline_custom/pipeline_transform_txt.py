from pyspark.sql import functions as f
from pipeline_step.pipeline_step import PipelineStep
from pyspark.sql.functions import when

class PipelineTransformTxt(PipelineStep):
    def __init__(self):
        super().__init__()
        print('transform data')

    def run(self, spark, params, df):
     
        df = df.withColumn('unitPrice',  df.price / df.quantity )
        drop_cols = ['team_code','description', 'TS_id', 'order_id','player_name', 'player_number','order_time']
        df = df.drop(*drop_cols)
   
        return df