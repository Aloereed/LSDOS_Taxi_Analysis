## visualization 的使用
打开cmd, 运行命令 python [main.py](https://github.com/Aloereed/LSDOS_Taxi_Analysis/blob/main/visualization/main.py) 

搭建本地服务器，等到出现如下内容时
```
 * Serving Flask app "main" (lazy loading)
 * Environment: production
   WARNING: This is a development server. Do not use it in a production deployment.
   Use a production WSGI server instead.
 * Debug mode: off
 * Running on http://127.0.0.1:11666/ (Press CTRL+C to quit)
```
打开浏览器, 输入 url:  localhost:11666, 便可看到相应的可视化界面（需要打开VPN）


---

#### 可视化界面的 user guidance

在界面加载完毕后，首先点击按钮 **Load Data** 以加载 NYC各个行政区域的数据，加载完毕后，地图视图上会出现对NYC的分区结果。
随后，文本输入框输入0到23的整数,这个整数代表了你想要的数据是哪个小时的(e.g. 如果你输入20,并点击按钮 **Set**,那么代表你想探索20:00-21:00的相关数据)。点击 **Set** 后，等待一段时间，直到出现了弹窗提示，表示关于 pickUp 和 dropOff 的数据加载完成。如果输入范围之外的数字或者小数，系统后台是不会响应的。
至此,所需要的所有数据加载完毕后，可以用鼠标划过地图上的对应区域。鼠标悬停的区域的相关信息会显示在**左下角**,包含了该区域的ID, zone 和 borough 的信息。

交互项以及注意点
* 点击按钮 **HeatMap-pu**, 地图上会显示相应时间段内的乘车(pick-up)的热力图。颜色越深的区域表示该区域在相应时间段内的乘车数越多。同理，点击按钮 **HeatMap-do**, 地图上会显示相应时间段内的下车(drop-off)的热力图。
* 鼠标悬停在某个感兴趣的区域上, **单击** 该区域, 等待一段时间, 地图上的分区会出现不同深浅的颜色, 表示的含义是：在当前时间段下，以我点击的区域为 **目的地** 的所有trip中所涉及到的所有出发地。某一区域的颜色越深，表示该时间段从这个区域出发到达所点击区域的 taxi trip 数量越多。反之, **长按** 一个区域，会显示以该区域为 **出发地** 的相关信息。
* 需要注意的是, 每次单击或长按某一区域时，需要首先点击按钮 **Clean** 以清除掉地图上的所有颜色, 以免产生混淆。
