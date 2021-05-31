from pyspark.sql import SparkSession
import os.path


class TaxiData:
    def __init__(self):
        self.spark = SparkSession.builder.getOrCreate()
        self.data = self.spark.read.csv('Taxidata_20180501_20180526.csv', header=True)
        self.data.createOrReplaceTempView('data')


    def time_trans_count(self, start_time, end_time):
        '''
        Given start time and end time, returns
        :param start_time, end_time: "yyyy-mm-dd hh:mm:ss"
        :return: dataframe object. columns: location_id, pickup_counts, dropoff_counts
        '''

        query = '''
            select location_id, sum(pickup_count) as pickup_count, sum(dropoff_count) as dropoff_count from ( 
            select puLocationId as location_id, 1 as pickup_count, 0 as dropoff_count from data 
            where timestamp("{start_time}") < tpepPickupDateTime and tpepPickupDateTime < timestamp("{end_time}")
            union all select doLocationId, 0, 1 from data 
            where timestamp("{start_time}") < tpepDropoffDateTime and tpepDropoffDateTime < timestamp("{end_time}")
            ) 
            group by location_id 
        '''.format(start_time=start_time, end_time=end_time)
        return self.spark.sql(query)

    def time_trans_count_hourly(self, given_hour):
        '''
        Given start time and end time, returns hourly statistic
        :param start_time, end_hour: int
        :return: dataframe object. columns: location_id, pickup_counts, dropoff_counts
        '''
        # Feature: if an area has no pick-up and drop-off in the given time, it would not appear in the csv file.
        query = '''
            select location_id, sum(pickup_count) as pickup_count, sum(dropoff_count) as dropoff_count from ( 
            select puLocationId as location_id, 1 as pickup_count, 0 as dropoff_count from data 
            where {given_hour} == hour(tpepPickupDateTime)
            union all select doLocationId, 0, 1 from data 
            where {given_hour} == hour(tpepDropoffDateTime)
            ) 
            group by location_id 
        '''.format(given_hour=given_hour)
        return self.spark.sql(query)


if __name__ == '__main__':
    # start_time = ""
    # end_time = ""
    # test = TaxiData()
    # df = test.time_trans_count(start_time, end_time)
    # #pd_df.to_csv('all_data-'+str(start_time)+'->'+str(end_time)+'.csv')
    #
    # # 出发地排序
    # print('Top 10 Popular Places of Departure('+str(start_time)+'->'+str(end_time)+')')
    # df.sort(df.pickup_count.desc()).show(10)
    # print('\n')
    # #sortByPick.to_csv('pickup_sort-'+str(start_time)+'->'+str(end_time)+'.csv', index = False, columns=['location_id','pickup_count'])
    #
    # # 目的地排序
    # print('Top Ten Popular Destinations('+str(start_time)+'->'+str(end_time)+')')
    # df.sort(df.dropoff_count.desc()).show(10)
    # print('\n')
    # #sortByDrop.to_csv('dropoff_sort-'+str(start_time)+'->'+str(end_time)+'.csv', index = False, columns=['location_id','dropoff_count'])

    dataset = TaxiData()
    for i in range(24):
        df = dataset.time_trans_count_hourly(i)

        print("Logging things within hour {}.".format(i))
        df.sort(df.pickup_count.desc())\
            .toPandas()\
            .to_csv(os.path.join('hourly_statistic_2018', 'pickup_2018_{}_to_{}.csv'.format(i, i+1)), header=True, index=False)
        df.sort(df.dropoff_count.desc())\
            .toPandas()\
            .to_csv(os.path.join('hourly_statistic_2018', 'dropoff_2018_{}_to_{}.csv'.format(i, i+1)), header=True, index=False)


