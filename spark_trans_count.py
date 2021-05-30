from pyspark.sql import SparkSession

# Query parameter here.
start_time = "2019-05-01 11:30:00"
end_time = "2019-05-01 15:00:00"

class TaxiData:
    def __init__(self):
        self.spark = SparkSession.builder.getOrCreate()
        self.data = self.spark.read.csv('Taxidata_20190501_20190602.csv', header=True)
        self.data.createOrReplaceTempView('data')

    # def time_routes_count(self, start_time, end_time):
    #     '''
    #     Given start_time and end_time, returns all routes within the interval.
    #     :param start_time, end_time: "yyyy-mm-dd hh:mm:ss"
    #     :return: dataframe object. columns: number, route
    #     '''
    #
    #     query = '''
    #         select count(vendorID) as number, (puLocationId, doLocationId) as route from data
    #         where timestamp("{start_time}") < tpepPickupDateTime and tpepDropoffDateTime < timestamp("{end_time}")
    #         group by (puLocationId, doLocationId)
    #     '''.format(start_time=start_time, end_time=end_time)
    #     return self.spark.sql(query)

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


if __name__ == '__main__':
    test = TaxiData()
    df = test.time_trans_count(start_time, end_time)
    #pd_df.to_csv('all_data-'+str(start_time)+'->'+str(end_time)+'.csv')

    # 出发地排序
    print('Top 10 Popular Places of Departure('+str(start_time)+'->'+str(end_time)+')')
    df.sort(df.pickup_count.desc()).show(10)
    print('\n')
    #sortByPick.to_csv('pickup_sort-'+str(start_time)+'->'+str(end_time)+'.csv', index = False, columns=['location_id','pickup_count'])

    # 目的地排序
    print('Top Ten Popular Destinations('+str(start_time)+'->'+str(end_time)+')')
    df.sort(df.dropoff_count.desc()).show(10)
    print('\n')
    #sortByDrop.to_csv('dropoff_sort-'+str(start_time)+'->'+str(end_time)+'.csv', index = False, columns=['location_id','dropoff_count'])

    #print('More records can be found in the csv files outputed in this directory')

