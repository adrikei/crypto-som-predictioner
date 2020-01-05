#We found this library to extract the data
import requests
from bs4 import BeautifulSoup

#initial date in loop
startTime = 1524182400000 #may 01 2018
#final date in loop
startTime_ = startTime+60000*60
#your path
path = "history_BTCUSDT"
#while loop get the data until last date
while startTime < 1527811201000: #1527811201000 #jun 01 2018
    
    #site url + start finish
    url = ('https://api.binance.com/api/v1/aggTrades?symbol=BTCUSDT&startTime=%s&endTime=%s')%(startTime, startTime_)
    #get data
    page1 = requests.get(url)
    #transform data
    soup1 = BeautifulSoup(page1.content, 'html.parser')
    #organize data
    soup2 = soup1.prettify()
    #split to a list
    soup3 = soup2.split('}',100000)
    #create and open file
    with open(path, "a") as my_file:
        #start a for loop lenght of the list
        for i in range(len(soup3)-1):
            #separate element by element from the list and assign to a variable
            x = soup3[i]
            #assign to same variable and replace same characters
            x = x.replace(',{','{').replace('[{','{')
            #save in file line by line
            my_file.write('{}\n'.format(x+'}'))
        #last date from a for loop to start while loop again
        startTime = int(x.split('"T":',1)[1][0:13])
        #final date in loop based on inicial date
        startTime_ = startTime+60000*60
#close file
my_file.close()