### 一、distance 
​     DOLocationID；avg_dist；sd_dist
​            表示 地理位置、平均距离、标准差

### 二、order_count

  #### 1、hour表
​        DOLocationID; dohour;	mean_hour;  sd_hour
​           表示  地理位置、 一天中的某一个小时、这个小时平均订单、根据一年的数据算出这个位置这个
​                  小时订单数量标准差（偏远地区就1条记录可能出现NaN值）

  #### 2、day表：
​      DOLocationID；doday；mean_day；sd_day
​          表示  地理位置、 一周中的某一个天、星期X平均订单数、根据一年的数据算出星期X订单数标准差

### 三、speed
 #### 1、hour表
​     sp_hour； mean_speed；	sd_speed
​         表示 一天中的某个小时、该小时平均速度、标准差

 #### 2、day表
​     sp_day；mean_speed；sd_speed
​         表示 星期X、星期X平均速度、根据一年中不同星期算出星期X标准差

#### 3、hourloc pu表
​       PULocationID,puhour,avg_speed,sd_speed
​      表示 每个出发地、 每小时出发、平均速度和标准差

#### 4、hourloc do表
​      同理为到达地

#### 5、route表
​      PULocationID,DOLocationID,puhour,avg_speed,sd_speed
​      表示根据出发地和到达点确定一个路线 求每小时速度（和标准差）

### 四. Payment 文件夹

- chisq_result.md
    将支付方式和路程距离的关系，与总价与小费的关系，分别作出列联表后进行卡方检验所得的结果。
    （两个检验得到的p值都是0，也就是说两个因素不独立）

关于支付方式和路程距离关系的统计
- paytype_dist.csv
    不同支付方式(payment)对应的路程距离(dist)的均值和方差

关于总价与小费关系的统计
- price_tip_data.csv
    总价对应的小费的频次统计表。纵轴为总价金额所在的区间，横轴为小费金额所在的区间。
    可以用于画直方图

- price_tip_stat.csv
    总价对应的小费的均值和方差信息