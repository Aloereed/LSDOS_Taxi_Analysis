import os 
resultList = os.listdir('/home/hadoop/pj/analyze_pattern/output/output')  
resultList.sort()  
 
fname = open('/home/hadoop/pj/analyze_pattern/output/month_do_tpep.csv', "w")   
 
for i in resultList:
    x = open ('/home/hadoop/pj/analyze_pattern/output/output/{0}'.format(i),  "r")   
    fname.write(x.read())  
    x.close()    
    
fname.close()
