from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from fbprophet import Prophet
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime

start_time = datetime.now()

spark = SparkSession.builder.appName('testML').master("local[*]").getOrCreate()
result_schema = StructType([
    StructField('ds', TimestampType()),
    StructField('car_park_no', StringType()),
    StructField('latitude', StringType()),
    StructField('longitude', StringType()),
    StructField('total_lots', StringType()),
    StructField('y', IntegerType()),
    StructField('yhat', IntegerType()),
    StructField('yhat_upper', IntegerType()),
    StructField('yhat_lower', IntegerType()),
])

@pandas_udf(result_schema, PandasUDFType.GROUPED_MAP)
def forecast_result(carpark_pd):
    # define model parameter
    model = Prophet(interval_width=0.95, growth='linear', daily_seasonality=True)
    # model fitting
    model.fit(carpark_pd)
    # make future dataframe with specified period and frequency
    future_pd = model.make_future_dataframe(periods=48, freq='30min', include_history=True)
    # perform forecasting
    forecast_pd = model.predict(future_pd)
    # convert negative values to zero
    num = forecast_pd._get_numeric_data()
    num[num < 0] = 0
    # join forecasted data with existing attributes
    f_pd = forecast_pd[['ds', 'yhat', 'yhat_upper', 'yhat_lower']].set_index('ds')
    cp_pd = carpark_pd[['ds', 'car_park_no', 'y', 'latitude', 'longitude', 'total_lots']].set_index('ds')
    result_pd = f_pd.join(cp_pd, how='left')
    result_pd.reset_index(level=0, inplace=True)
    result_pd['car_park_no'] = carpark_pd['car_park_no'].iloc[0]
    result_pd['latitude'] = carpark_pd['latitude'].iloc[0]
    result_pd['longitude'] = carpark_pd['longitude'].iloc[0]
    result_pd['total_lots'] = carpark_pd['total_lots'].iloc[0]
    return result_pd[['ds', 'car_park_no', 'latitude', 'longitude', 'total_lots', 'y', 'yhat', 'yhat_upper', 'yhat_lower']]


# load data into panda dataframe and convert ds column to date time
path = '/Users/matthew.yap/Documents/GoPratice/ebd-carpark-availability-producer/carpark-all-location-today.csv'
df = pd.read_csv(path)
# data conversion
df['ds'] = pd.to_datetime(df['ds'])
df['latitude'] = df['latitude'].astype(str)
df['longitude'] = df['longitude'].astype(str)
df['total_lots'] = df['total_lots'].astype(str)
# Convert to Spark dataframe
sdf = spark.createDataFrame(df)
sdf.printSchema()
sdf.show(5)
sdf.count()
# Repartition dataframe by carpark no
carparkdf = sdf.repartition(spark.sparkContext.defaultParallelism, ['car_park_no']).cache()

# Apply time series forecasting
results = (carparkdf.groupby('car_park_no').apply(forecast_result).withColumn('training_date', current_date()))
results.cache()
results.show()

# Save results to csv
results.write.option("header", "true").mode('overwrite').format('csv').save('./testdata_all_location_total_lots')

# Visualize Some data
# results.coalesce(1)
# print("total:", results.count(), "rows")
# results.createOrReplaceTempView('forecasted')
# spark.sql("SELECT car_park_no, count(*) FROM  forecasted GROUP BY car_park_no").show()
# final_df = results.toPandas()
#
# # display the chart
# final_df = final_df.set_index('ds')
# final_df.query('car_park_no == "A100"')[['y', 'yhat']].plot()
# plt.show()
#
# final_df.query('car_park_no == "A15"')[['y', 'yhat']].plot()
# plt.show()

print('Duration: {}'.format(datetime.now() - start_time))
