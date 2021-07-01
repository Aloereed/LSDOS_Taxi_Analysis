# 大规模分布式系统 - plot

setwd('D:/2021_Spring/大规模分布式系统/project/visualization')

library(ggplot2)
library(ggpmisc)
library(scales)
library(cowplot)
library(showtext)
library(dplyr)

# 绘图函数
multiplot <- function(..., plotlist=NULL, file, cols=1, layout=NULL) {
  library(grid)
  
  # Make a list from the ... arguments and plotlist
  plots <- c(list(...), plotlist)
  
  numPlots = length(plots)
  
  # If layout is NULL, then use 'cols' to determine layout
  if (is.null(layout)) {
    # Make the panel
    # ncol: Number of columns of plots
    # nrow: Number of rows needed, calculated from # of cols
    layout <- matrix(seq(1, cols * ceiling(numPlots/cols)),
                     ncol = cols, nrow = ceiling(numPlots/cols))
  }
  
  if (numPlots==1) {
    print(plots[[1]])
    
  } else {
    # Set up the page
    grid.newpage()
    pushViewport(viewport(layout = grid.layout(nrow(layout), ncol(layout))))
    
    # Make each plot, in the correct location
    for (i in 1:numPlots) {
      # Get the i,j matrix positions of the regions that contain this subplot
      matchidx <- as.data.frame(which(layout == i, arr.ind = TRUE))
      
      print(plots[[i]], vp = viewport(layout.pos.row = matchidx$row,
                                      layout.pos.col = matchidx$col))
    }
  }
}

# 订单量与时间的分布关系
order.day.pu <- read.csv('./static/data/order_time/pu_day.csv')
order.hour.pu <- read.csv('./static/data/order_time/pu_hour.csv')
order.day.do <- read.csv('./static/data/order_time/do_day.csv')
order.hour.do <- read.csv('./static/data/order_time/do_hour.csv')

# JFK Airport - 132
# Newark Airport - 1
# LaGuardia Airport - 138
g1<-ggplot()+
  geom_line(aes(x=puhour,y=mean_hour,color = "red", linetype = "4")
            ,data=filter(order.hour.pu,PULocationID==132)) +
  geom_line(aes(x=puhour,y=mean_hour,color = "blue", linetype = "2")
            ,data=filter(order.hour.pu,PULocationID==1)) +
  geom_line(aes(x=puhour,y=mean_hour,color = "purple", linetype = "1")
            ,data=filter(order.hour.pu,PULocationID==138)) +
  scale_color_manual(name = "group",
                     values = c('red' = 'red', "blue" = 'blue', "purple"="purple"), 
                     breaks = c("red", "blue", "purple"),
                     labels = c('JFK Airport', 'Newark Airport', 'LaGuardia Airport')) +
  scale_linetype_manual(name = "group",
                        values = c("4" = 4, "2" = 2, "1"=1), 
                        breaks = c("4","2","1"),
                        labels = c('JFK Airport', 'Newark Airport', 'LaGuardia Airport')) +
  labs(x="hours",y="average orders",title = "Order's Distributions over the Hours in a Day - Pick Up") + 
  theme(legend.title=element_blank(),legend.position = c(0.15, 0.875)) +
  theme(legend.text = element_text(colour = 'black',face = 'bold',size=10))

g2<-ggplot()+
  geom_line(aes(x=dohour,y=mean_hour,color = "red", linetype = "4")
            ,data=filter(order.hour.do,DOLocationID==132)) +
  geom_line(aes(x=dohour,y=mean_hour,color = "blue", linetype = "2")
            ,data=filter(order.hour.do,DOLocationID==1)) +
  geom_line(aes(x=dohour,y=mean_hour,color = "purple", linetype = "1")
            ,data=filter(order.hour.do,DOLocationID==138)) +
  scale_color_manual(name = "group",
                     values = c('red' = 'red', "blue" = 'blue', "purple"="purple"), 
                     breaks = c("red", "blue", "purple"),
                     labels = c('JFK Airport', 'Newark Airport', 'LaGuardia Airport')) +
  scale_linetype_manual(name = "group",
                        values = c("4" = 4, "2" = 2, "1"=1), 
                        breaks = c("4","2","1"),
                        labels = c('JFK Airport', 'Newark Airport', 'LaGuardia Airport')) +
  labs(x="hours",y="average orders",title = "Order's Distributions over the Hours in a Day - Drop Off") + 
  theme(legend.title=element_blank(),legend.position = c(0.9, 0.875)) +
  theme(legend.text = element_text(colour = 'black',face = 'bold',size=10)) 


layout <- matrix(c(1,2), nrow = 2, byrow = TRUE)
multiplot(plotlist = list(g1, g2), layout = layout)

# Financial District North - 87
# Central Parl - 43
# Upper East Side South - 237
g1<-ggplot()+
  geom_line(aes(x=puhour,y=mean_hour,color = "red", linetype = "4")
            ,data=filter(order.hour.pu,PULocationID==87)) +
  geom_line(aes(x=puhour,y=mean_hour,color = "blue", linetype = "2")
            ,data=filter(order.hour.pu,PULocationID==43)) +
  geom_line(aes(x=puhour,y=mean_hour,color = "purple", linetype = "1")
            ,data=filter(order.hour.pu,PULocationID==237)) +
  scale_color_manual(name = "group",
                     values = c('red' = 'red', "blue" = 'blue', "purple"="purple"), 
                     breaks = c("red", "blue", "purple"),
                     labels = c('Financial District North', 'Central Parl', 'Upper East Side South')) +
  scale_linetype_manual(name = "group",
                        values = c("4" = 4, "2" = 2, "1"=1), 
                        breaks = c("4","2","1"),
                        labels = c('Financial District North', 'Central Parl', 'Upper East Side South')) +
  labs(x="hours",y="average orders",title = "Order's Distributions over the Hours in a Day - Pick Up") + 
  theme(legend.title=element_blank(),legend.position = c(0.15, 0.875)) +
  theme(legend.text = element_text(colour = 'black',face = 'bold',size=10))

g2<-ggplot()+
  geom_line(aes(x=dohour,y=mean_hour,color = "red", linetype = "4")
            ,data=filter(order.hour.do,DOLocationID==87)) +
  geom_line(aes(x=dohour,y=mean_hour,color = "blue", linetype = "2")
            ,data=filter(order.hour.do,DOLocationID==43)) +
  geom_line(aes(x=dohour,y=mean_hour,color = "purple", linetype = "1")
            ,data=filter(order.hour.do,DOLocationID==237)) +
  scale_color_manual(name = "group",
                     values = c('red' = 'red', "blue" = 'blue', "purple"="purple"), 
                     breaks = c("red", "blue", "purple"),
                     labels = c('Financial District North', 'Central Parl', 'Upper East Side South')) +
  scale_linetype_manual(name = "group",
                        values = c("4" = 4, "2" = 2, "1"=1), 
                        breaks = c("4","2","1"),
                        labels = c('Financial District North', 'Central Parl', 'Upper East Side South')) +
  labs(x="hours",y="average orders",title = "Order's Distributions over the Hours in a Day - Drop Off") + 
  theme(legend.title=element_blank(),legend.position = c(0.15, 0.875)) +
  theme(legend.text = element_text(colour = 'black',face = 'bold',size=10)) 

layout <- matrix(c(1,2), nrow = 2, byrow = TRUE)
multiplot(plotlist = list(g1, g2), layout = layout)

# Woodlawn/Wakefield - 259
# Eltingville/Annadale/Prince's Bay - 84
# Glen Oaks - 101
g1<-ggplot()+
  geom_line(aes(x=puhour,y=mean_hour,color = "red", linetype = "4")
            ,data=filter(order.hour.pu,PULocationID==259)) +
  geom_line(aes(x=puhour,y=mean_hour,color = "blue", linetype = "2")
            ,data=filter(order.hour.pu,PULocationID==84)) +
  geom_line(aes(x=puhour,y=mean_hour,color = "purple", linetype = "1")
            ,data=filter(order.hour.pu,PULocationID==101)) +
  scale_color_manual(name = "group",
                     values = c('red' = 'red', "blue" = 'blue', "purple"="purple"), 
                     breaks = c("red", "blue", "purple"),
                     labels = c('Woodlawn/Wakefield', "Eltingville/Annadale/Prince's Bay"
                                , 'Glen Oaks')) +
  scale_linetype_manual(name = "group",
                        values = c("4" = 4, "2" = 2, "1"=1), 
                        breaks = c("4","2","1"),
                        labels = c('Woodlawn/Wakefield', "Eltingville/Annadale/Prince's Bay"
                                   , 'Glen Oaks')) +
  labs(x="hours",y="average orders",title = "Order's Distributions over the Hours in a Day - Pick Up") + 
  theme(legend.title=element_blank(),legend.position = c(0.85, 0.875)) +
  theme(legend.text = element_text(colour = 'black',face = 'bold',size=10))

g2<-ggplot()+
  geom_line(aes(x=dohour,y=mean_hour,color = "red", linetype = "4")
            ,data=filter(order.hour.do,DOLocationID==259)) +
  geom_line(aes(x=dohour,y=mean_hour,color = "blue", linetype = "2")
            ,data=filter(order.hour.do,DOLocationID==84)) +
  geom_line(aes(x=dohour,y=mean_hour,color = "purple", linetype = "1")
            ,data=filter(order.hour.do,DOLocationID==101)) +
  scale_color_manual(name = "group",
                     values = c('red' = 'red', "blue" = 'blue', "purple"="purple"), 
                     breaks = c("red", "blue", "purple"),
                     labels = c('Woodlawn/Wakefield', "Eltingville/Annadale/Prince's Bay"
                                , 'Glen Oaks')) +
  scale_linetype_manual(name = "group",
                        values = c("4" = 4, "2" = 2, "1"=1), 
                        breaks = c("4","2","1"),
                        labels = c('Woodlawn/Wakefield', "Eltingville/Annadale/Prince's Bay"
                                   , 'Glen Oaks')) +
  labs(x="hours",y="average orders",title = "Order's Distributions over the Hours in a Day - Drop Off") + 
  theme(legend.title=element_blank(),legend.position = c(0.45, 0.875)) +
  theme(legend.text = element_text(colour = 'black',face = 'bold',size=10)) 

layout <- matrix(c(1,2), nrow = 2, byrow = TRUE)
multiplot(plotlist = list(g1, g2), layout = layout)



# JFK Airport - 132
# Newark Airport - 1
# LaGuardia Airport - 138
g1<-ggplot()+
  geom_line(aes(x=puday,y=mean_day,color = "red", linetype = "4")
            ,data=filter(order.day.pu,PULocationID==132)) +
  geom_line(aes(x=puday,y=mean_day,color = "blue", linetype = "2")
            ,data=filter(order.day.pu,PULocationID==1)) +
  geom_line(aes(x=puday,y=mean_day,color = "purple", linetype = "1")
            ,data=filter(order.day.pu,PULocationID==138)) +
  scale_color_manual(name = "group",
                     values = c('red' = 'red', "blue" = 'blue', "purple"="purple"), 
                     breaks = c("red", "blue", "purple"),
                     labels = c('JFK Airport', 'Newark Airport', 'LaGuardia Airport')) +
  scale_linetype_manual(name = "group",
                        values = c("4" = 4, "2" = 2, "1"=1), 
                        breaks = c("4","2","1"),
                        labels = c('JFK Airport', 'Newark Airport', 'LaGuardia Airport')) +
  labs(x="days",y="average orders",title = "Order's Distributions over the Days in a Week - Pick Up") + 
  theme(legend.title=element_blank(),legend.position = c(0.15, 0.6)) +
  theme(legend.text = element_text(colour = 'black',face = 'bold',size=10))

g2<-ggplot()+
  geom_line(aes(x=doday,y=mean_day,color = "red", linetype = "4")
            ,data=filter(order.day.do,DOLocationID==132)) +
  geom_line(aes(x=doday,y=mean_day,color = "blue", linetype = "2")
            ,data=filter(order.day.do,DOLocationID==1)) +
  geom_line(aes(x=doday,y=mean_day,color = "purple", linetype = "1")
            ,data=filter(order.day.do,DOLocationID==138)) +
  scale_color_manual(name = "group",
                     values = c('red' = 'red', "blue" = 'blue', "purple"="purple"), 
                     breaks = c("red", "blue", "purple"),
                     labels = c('JFK Airport', 'Newark Airport', 'LaGuardia Airport')) +
  scale_linetype_manual(name = "group",
                        values = c("4" = 4, "2" = 2, "1"=1), 
                        breaks = c("4","2","1"),
                        labels = c('JFK Airport', 'Newark Airport', 'LaGuardia Airport')) +
  labs(x="days",y="average orders",title = "Order's Distributions over the Days in a Week - Drop Off") + 
  theme(legend.title=element_blank(),legend.position = c(0.9, 0.875)) +
  theme(legend.text = element_text(colour = 'black',face = 'bold',size=10)) 

layout <- matrix(c(1,2), nrow = 2, byrow = TRUE)
multiplot(plotlist = list(g1, g2), layout = layout)

# Financial District North - 87
# Time Square - 230
# Central Parl - 43
# Upper East Side South - 237
g1<-ggplot()+
  geom_line(aes(x=puday,y=mean_day,color = "red", linetype = "4")
            ,data=filter(order.day.pu,PULocationID==87)) +
  geom_line(aes(x=puday,y=mean_day,color = "black", linetype = "5")
            ,data=filter(order.day.pu,PULocationID==230)) +
  geom_line(aes(x=puday,y=mean_day,color = "blue", linetype = "2")
            ,data=filter(order.day.pu,PULocationID==43)) +
  geom_line(aes(x=puday,y=mean_day,color = "purple", linetype = "1")
            ,data=filter(order.day.pu,PULocationID==237)) +
  scale_color_manual(name = "group",
                     values = c('red' = 'red',"black"="black","blue" = 'blue', "purple"="purple"), 
                     breaks = c("red","black", "blue", "purple"),
                     labels =c('Financial District North','Time Square'
                               , 'Central Parl', 'Upper East Side South')) +
  scale_linetype_manual(name = "group",
                        values = c("4" = 4,"5"=5, "2" = 2, "1"=1), 
                        breaks = c("4","5","2","1"),
                        labels = c('Financial District North','Time Square'
                                   , 'Central Parl', 'Upper East Side South')) +
  labs(x="days",y="average orders",title = "Order's Distributions over the Days in a Week - Pick Up") + 
  theme(legend.title=element_blank(),legend.position = c(0.1, 0.85)) +
  theme(legend.text = element_text(colour = 'black',face = 'bold',size=10))

g2<-ggplot()+
  geom_line(aes(x=doday,y=mean_day,color = "red", linetype = "4")
            ,data=filter(order.day.do,DOLocationID==87)) +
  geom_line(aes(x=doday,y=mean_day,color = "black", linetype = "5")
            ,data=filter(order.day.do,DOLocationID==230)) +
  geom_line(aes(x=doday,y=mean_day,color = "blue", linetype = "2")
            ,data=filter(order.day.do,DOLocationID==43)) +
  geom_line(aes(x=doday,y=mean_day,color = "purple", linetype = "1")
            ,data=filter(order.day.do,DOLocationID==237)) +
  scale_color_manual(name = "group",
                     values = c('red' = 'red',"black"="black","blue" = 'blue', "purple"="purple"), 
                     breaks = c("red","black", "blue", "purple"),
                     labels = c('Financial District North','Time Square'
                                , 'Central Parl', 'Upper East Side South')) +
  scale_linetype_manual(name = "group",
                        values = c("4" = 4,"5"=5, "2" = 2, "1"=1), 
                        breaks = c("4","5","2","1"),
                        labels = c('Financial District North','Time Square'
                                   , 'Central Parl', 'Upper East Side South')) +
  labs(x="days",y="average orders",title = "Order's Distributions over the Days in a Week - Drop Off") + 
  theme(legend.title=element_blank(),legend.position = c(0.95, 0.875)) +
  theme(legend.text = element_text(colour = 'black',face = 'bold',size=10)) 


layout <- matrix(c(1,2), nrow = 2, byrow = TRUE)
multiplot(plotlist = list(g1, g2), layout = layout)

# Woodlawn/Wakefield - 259
# Eltingville/Annadale/Prince's Bay - 84
# Glen Oaks - 101
g1<-ggplot()+
  geom_line(aes(x=puday,y=mean_day,color = "red", linetype = "4")
            ,data=filter(order.day.pu,PULocationID==259)) +
  geom_line(aes(x=puday,y=mean_day,color = "blue", linetype = "2")
            ,data=filter(order.day.pu,PULocationID==84)) +
  geom_line(aes(x=puday,y=mean_day,color = "purple", linetype = "1")
            ,data=filter(order.day.pu,PULocationID==101)) +
  scale_color_manual(name = "group",
                     values = c('red' = 'red', "blue" = 'blue', "purple"="purple"), 
                     breaks = c("red", "blue", "purple"),
                     labels = c('Woodlawn/Wakefield', "Eltingville/Annadale/Prince's Bay"
                                , 'Glen Oaks')) +
  scale_linetype_manual(name = "group",
                        values = c("4" = 4, "2" = 2, "1"=1), 
                        breaks = c("4","2","1"),
                        labels = c('Woodlawn/Wakefield', "Eltingville/Annadale/Prince's Bay"
                                   , 'Glen Oaks')) +
  labs(x="days",y="average orders",title = "Order's Distributions over the Days in a Week - Pick Up") + 
  theme(legend.title=element_blank(),legend.position = c(0.15, 0.4)) +
  theme(legend.text = element_text(colour = 'black',face = 'bold',size=10))

g2<-ggplot()+
  geom_line(aes(x=doday,y=mean_day,color = "red", linetype = "4")
            ,data=filter(order.day.do,DOLocationID==259)) +
  geom_line(aes(x=doday,y=mean_day,color = "blue", linetype = "2")
            ,data=filter(order.day.do,DOLocationID==84)) +
  geom_line(aes(x=doday,y=mean_day,color = "purple", linetype = "1")
            ,data=filter(order.day.do,DOLocationID==101)) +
  scale_color_manual(name = "group",
                     values = c('red' = 'red', "blue" = 'blue', "purple"="purple"), 
                     breaks = c("red", "blue", "purple"),
                     labels = c('Woodlawn/Wakefield', "Eltingville/Annadale/Prince's Bay"
                                , 'Glen Oaks')) +
  scale_linetype_manual(name = "group",
                        values = c("4" = 4, "2" = 2, "1"=1), 
                        breaks = c("4","2","1"),
                        labels = c('Woodlawn/Wakefield', "Eltingville/Annadale/Prince's Bay"
                                   , 'Glen Oaks')) +
  labs(x="days",y="average orders",title = "Order's Distributions over the Days in a Week - Drop Off") + 
  theme(legend.title=element_blank(),legend.position = c(0.5, 0.875)) +
  theme(legend.text = element_text(colour = 'black',face = 'bold',size=10)) 

layout <- matrix(c(1,2), nrow = 2, byrow = TRUE)
multiplot(plotlist = list(g1, g2), layout = layout)

# 路程长度与地域的关系
# pu_dist_location

distance.loc.pu <- read.csv('./static/data/distance_data/pu_dist_location.csv')
distance.loc.do <- read.csv('./static/data/distance_data/do_dist_location.csv')
names(distance.loc.pu)[1]<-'LocationID'
names(distance.loc.do)[1]<-'LocationID'
zone <- read.csv('./static/data/taxi_zones.csv') %>% select(LocationID,zone,borough)
names(distance.loc.pu)
names(zone)
distance.loc.pu <- inner_join(distance.loc.pu,zone)
distance.loc.do <- inner_join(distance.loc.do,zone)
head(distance.loc.pu)
head(distance.loc.do)

g1<-ggplot(data=rbind(head(distance.loc.pu,6),tail(distance.loc.pu,6)),mapping=aes(x=reorder(zone,-avg_dist),y=avg_dist))+
  geom_bar(stat="identity",width = .8) +
  theme(axis.text.x = element_text(angle = 20, hjust =1))+
  geom_hline(aes(yintercept=mean(distance.loc.pu$avg_dist),color='red')) +
  geom_hline(aes(yintercept=median(distance.loc.pu$avg_dist),color='blue')) +
  scale_color_manual(name = "group",
                     values = c('red' = 'red', "blue" = 'blue'), 
                     breaks = c("red", "blue"),
                     labels = c('mean', "median")) +
  theme(legend.title=element_blank(),legend.position = c(0.9, 0.875)) +
  theme(legend.text = element_text(colour = 'black',face = 'bold',size=10)) +
  labs(x="zones",y="average order distances",title = "Order Distances' Distributions over the Zones  - Pick Up")
g1

g2<-ggplot(data=rbind(head(distance.loc.do,6),tail(distance.loc.do,6)),mapping=aes(x=reorder(zone,-avg_dist),y=avg_dist))+
  geom_bar(stat="identity",width = .8) +
  theme(axis.text.x = element_text(angle = 20, hjust =.7))+
  geom_hline(aes(yintercept=mean(distance.loc.do$avg_dist),color='red')) +
  geom_hline(aes(yintercept=median(distance.loc.do$avg_dist),color='blue')) +
  scale_color_manual(name = "group",
                     values = c('red' = 'red', "blue" = 'blue'), 
                     breaks = c("red", "blue"),
                     labels = c('mean', "median")) +
  theme(legend.title=element_blank(),legend.position = c(0.9, 0.875)) +
  theme(legend.text = element_text(colour = 'black',face = 'bold',size=10)) +
  labs(x="zones",y="average order distances",title = "Order Distances' Distributions over the Zones  - Drop Off")
g2

# 车速与时间的关系
speed.day <- read.csv('./static/data/speed_data/dayspeed.csv')
speed.hour <- read.csv('./static/data/speed_data/hourspeed.csv')
speed.route <- read.csv('./static/data/speed_data/route_speed.csv')
names(speed.hour)
names(speed.day)

g1<-ggplot()+
  geom_ribbon(aes(x = sp_hour, y = mean_speed, ymin = mean_speed-1.96*sd_speed, ymax = mean_speed+1.96*sd_speed)
              , data = speed.hour, fill = "lightskyblue1") +
  geom_line(aes(x=sp_hour,y=mean_speed)
            ,data=speed.hour) +
  labs(x="Hour in a Day",y="Average Speed",title = "Speed's Distributions over the Hours in a Day")
  # scale_color_manual(name = "group",
  #                    values = c('red' = 'red', "blue" = 'blue', "purple"="purple"), 
  #                    breaks = c("red", "blue", "purple"),
  #                    labels = c('Woodlawn/Wakefield', "Eltingville/Annadale/Prince's Bay"
  #                               , 'Glen Oaks')) +
  # scale_linetype_manual(name = "group",
  #                       values = c("4" = 4, "2" = 2, "1"=1), 
  #                       breaks = c("4","2","1"),
  #                       labels = c('Woodlawn/Wakefield', "Eltingville/Annadale/Prince's Bay"
  #                                  , 'Glen Oaks')) +
  # labs(x="days",y="average orders",title = "Order's Distributions over the Days in a Week - Drop Off") + 
  # theme(legend.title=element_blank(),legend.position = c(0.5, 0.875)) +
  # theme(legend.text = element_text(colour = 'black',face = 'bold',size=10)) 
g2<-ggplot()+
  geom_ribbon(aes(x = sp_day, y = mean_speed, ymin = mean_speed-1.96*sd_speed, ymax = mean_speed+1.96*sd_speed)
              , data = speed.day, fill = "lightskyblue1") +
  geom_line(aes(x=sp_day,y=mean_speed)
            ,data=speed.day) +
  labs(x="Day in a Week",y="Average Speed",title = "Speed's Distributions over the Days in a Week")

layout <- matrix(c(1,2), nrow = 2, byrow = TRUE)
multiplot(plotlist = list(g1, g2), layout = layout)

# 路程长度与支付方式的关系

# 支付方式与距离的关系图
pay.dist <- read.csv('./static/data/payment/paytype_dist.csv')
pay.dist$payment_type <- recode(pay.dist$payment_type,
                                `1`='Credit Card',
                                `2`='Cash',
                                `3`='No Charge',
                                `4`='Dispute',
                                `5`='Unknown')
pay.dist$payment_type
g<-ggplot(data=pay.dist,mapping=aes(x=payment_type,y=avg.dist.))+
  geom_bar(stat="identity",width = .8) +
  # theme(axis.text.x = element_text(angle = 20, hjust =1))
  # geom_hline(aes(yintercept=mean(distance.loc.pu$avg_dist),color='red')) +
  # geom_hline(aes(yintercept=median(distance.loc.pu$avg_dist),color='blue')) +
  # scale_color_manual(name = "group",
  #                    values = c('red' = 'red', "blue" = 'blue'), 
  #                    breaks = c("red", "blue"),
  #                    labels = c('mean', "median")) +
  # theme(legend.title=element_blank(),legend.position = c(0.9, 0.875)) +
  # theme(legend.text = element_text(colour = 'black',face = 'bold',size=10)) +
  labs(x="zones",y="average order distances",title = "Payment & Order's Distance")
g

# 小费占总价比例的分布
ratio.log <- read.csv('./static/data/payment/tip_ratio_rate_edited.csv')
names(ratio.log)[1] <- 'ratio'
names(ratio.log)
g<-ggplot(ratio.log,aes(x=ratio,y=sum.count.)) + 
  geom_bar(stat="identity") +
  labs(x="Ratio Intervals",y='Counts')+
  theme(axis.text.x = element_text(angle = 30, hjust =1))
  # theme(plot.title=element_text(hjust=0.5))+                          #调整字体，以及标题居中
  # stat_density(geom="line",size=1.5, color="grey")
g

# geom_histogram(alpha=0.5,binwidth=5)+                             #绘制直方图
#   labs(x='患者年龄', y="年龄分布密度", title="年龄分布密度图")+       #增加标签
#   theme(plot.title=element_text(hjust=0.5))+                          #调整字体，以及标题居中
#   stat_density(geom="line",size=1.5, color="grey")