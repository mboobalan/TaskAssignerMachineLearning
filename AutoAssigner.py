"""
 This is the  AutoAssigner Program that assigns the executives
 Methods below are :
 1.sendExecutivestoApp
 2.pearson_score
 3.getTopExecutiveList
 4.getTopExecutiveListML

"""

from flask import Flask,request
import os
import json
import pyodbc
from flask import jsonify
from math import sqrt,radians, cos, sin, asin
import pandas as pd
import numpy as np
import requests
app = Flask(__name__) 
#import project libraries here #

#global declaration dataframe here #
executiveDetails = pd.DataFrame()
ratingReport =  pd.DataFrame()
data = json.load(open('appsettings.json'))
connectionstring =data["connectionstring"]
limitCalculation = data["limitCalculation"]

#AutoAssigner - Routing #
@app.route('/AutoAssignerML' , methods=['POST'])
def sendExecutivestoApp():
    
    
    nearbyRadius= int(request.form.get('nearbyRadius'))
    customerPhoneNumber=str(request.form.get('taskShipToNumber'))
    pickupLatitude =str(float(request.form.get('pickupLatitude')))
    pickupLongitude =str(float(request.form.get('pickupLongitude')))
    executiveListLimit = int(request.form.get('executiveListLimit'))
    agentId  = int(request.form.get('agentId')) 
    taskId = int(request.form.get('taskId'))
    destinationLatitudeandLongitude =pickupLatitude +","+pickupLongitude

    global executiveDetails
    global ratingReport

    #SQL connection #
    connection = pyodbc.connect(connectionstring)
    cursor = connection.cursor()

    if(agentId == 0):
      executiveDetails = pd.read_sql_query("SELECT DISTINCT Latitude, Longitude, PartyRoleId FROM [Tracking].[ExecutiveLatestPingLocations] AS executivelatestping INNER JOIN [Tracking].[PingResults] AS pingresult ON executivelatestping.PingResultsId = pingresult.Id INNER JOIN [Entity].[PartyRoles] AS partyrole ON pingresult.PartyRoleId = partyrole.Id INNER JOIN [CodeList].[NonEditableCodeLists] AS noneditablecodelist ON noneditablecodelist.Id = partyrole.ContractTypeId WHERE partyrole.IsDontAssignMeEnabled = 'False' AND noneditablecodelist.Value = 'Freelancer' AND pingresult.PartyRoleId NOT IN (SELECT DISTINCT AssigneeId FROM [Task].[TaskRequests] WHERE TaskId = {assignedtaskId})".format(assignedtaskId = taskId), con=connection) 
    else:
      executiveDetails = pd.read_sql_query("SELECT DISTINCT Latitude, Longitude, PartyRoleId FROM [Tracking].[PingResults] AS ping INNER JOIN [Tracking].[ExecutiveLatestPingLocations] AS executivelatestping ON ping.Id = executivelatestping.PingResultsId INNER JOIN [Entity].[EntityRelationships] AS entityrelationship ON entityrelationship.ChildId = executivelatestping.DeliveryExecutiveId INNER JOIN [Entity].[PartyRoles] AS partyrole ON partyrole.Id = executivelatestping.DeliveryExecutiveId INNER JOIN [CodeList].[NonEditableCodeLists] AS relationshipconfirmation ON relationshipconfirmation.Id = entityrelationship.RelationShipStatusId INNER JOIN [CodeList].[NonEditableCodeLists] AS noneditablecodelist ON noneditablecodelist.Id = partyrole.ContractTypeId WHERE entityrelationship.ParentId = {deliveryAgentId} AND partyrole.DisplayName = 'DeliveryExecutive' AND partyrole.IsDontAssignMeEnabled ='false' AND noneditablecodelist.Value = 'AssociatedWithDeliveryAgent' AND noneditablecodelist.Discriminator = 'ContractType' AND relationshipconfirmation.Value = 'Approved' AND relationshipconfirmation.Discriminator = 'RelationShipStatus' AND ping.PartyRoleId NOT IN (SELECT DISTINCT AssigneeId FROM [Task].[TaskRequests] WHERE TaskId = {assignedtaskId})".format(deliveryAgentId=agentId,assignedtaskId = taskId), con=connection)

      executiveDetails.head()
    executiveDetails["PickUpDistanceForExecutive"] = 1;
    originLatitudeandLongitude =""
    pickUpDistanceForExecutive = [];
    ratingReport =  pd.read_sql_query("SELECT CustomerEmail, RatingValue,RatedToId FROM [Rating].[RatingHistories] WHERE  discriminator='CustomerRating'", con=connection)
  
  # Google distance matrix calculation #
    for index, source in executiveDetails.iterrows():
         unique_id = index
         
         lat = str(source['Latitude'])
         lon= str(source['Longitude'])+ '|'
         originLatitudeandLongitude += lat +","+ lon
    print(originLatitudeandLongitude)
    response = requests.get("http://maps.googleapis.com/maps/api/distancematrix/json?origins=%s&destinations=%s&mode=driving&language=en-EN&sensor=false"%(originLatitudeandLongitude,destinationLatitudeandLongitude))
    
    googleDistanceMatrixData = json.loads(response.text)

    for index, source in executiveDetails.iterrows():
        for isource, source in enumerate(googleDistanceMatrixData['origin_addresses']):
            for idestination, destination in enumerate(googleDistanceMatrixData['destination_addresses']):
                    row = googleDistanceMatrixData['rows'][isource]
                    cell = row['elements'][idestination]                   
                    pickUpDistanceForExecutive.append(round(cell['distance']['value'] /1000,2))
    
                    

    for index, source in executiveDetails.iterrows():                
       executiveDetails.loc[index, 'PickUpDistanceForExecutive'] =  pickUpDistanceForExecutive[index]
    print(executiveDetails.head)
    destinationLatitudeandLongitude =  pickupLatitude +","+ pickupLongitude
    
 
    executiveIdDataFromML = np.array(getTopExecutiveListML(customerPhoneNumber,nearbyRadius,destinationLatitudeandLongitude,pickupLongitude)).tolist()
    topExecutiveIdDataFromML = []
    for  executiveId in executiveIdDataFromML:
        #Executive tasklimit for ApprovedTaskLimit 50% reached logic( Freelancer Flow and Agent-Executive Flow)
        if(agentId == 0):
            taskLimitCount  = pd.read_sql_query("SELECT [Task].[TaskLimits].[ApprovedTaskLimit], [Task].[TasksDetailsCounts].[Assigned] ,[Task].[TaskLimits].[PartyRoleId]  FROM  [Task].[TaskLimits] INNER JOIN [Task].[TasksDetailsCounts]ON [Task].[TaskLimits].[PartyRoleId] = [Task].[TasksDetailsCounts].[PartyRoleId] where [Task].[TasksDetailsCounts].[PartyRoleId] = {deliveryExecutiveId}".format(deliveryExecutiveId=str(executiveId)),con=connection)
            isTaskLimitReached = (taskLimitCount.iloc[0]['ApprovedTaskLimit']/limitCalculation)  >= (taskLimitCount.iloc[0]['Assigned']) 
        else:
            taskLimitCount  = pd.read_sql_query("SELECT [Task].[TaskLimits].[PendingTaskLimit], [Task].[TasksDetailsCounts].[Pending] ,[Task].[TaskLimits].[PartyRoleId]  FROM  [Task].[TaskLimits] INNER JOIN [Task].[TasksDetailsCounts]ON [Task].[TaskLimits].[PartyRoleId] = [Task].[TasksDetailsCounts].[PartyRoleId] where [Task].[TasksDetailsCounts].[PartyRoleId] = {deliveryExecutiveId}".format(deliveryExecutiveId=str(executiveId)),con=connection)
            isTaskLimitReached = (taskLimitCount.iloc[0]['PendingTaskLimit']/limitCalculation)  >= (taskLimitCount.iloc[0]['Pending'])

        if (isTaskLimitReached):
            
            topExecutiveIdDataFromML.append(taskLimitCount.iloc[0]['PartyRoleId'])
    print(topExecutiveIdDataFromML)
    return jsonify(executiveIdFromML=np.array(topExecutiveIdDataFromML).tolist()[0:executiveListLimit]) 


#Collaborative Filtering using Pearson correlation Coefficient Algorithm (Unsupervised Learning)
def pearson_score(customer1,customer2):

    df_firstcustomerDetails = ratingReport.loc[ratingReport.CustomerEmail==customer1]
    df_secondcustomerDetails = ratingReport.loc[ratingReport.CustomerEmail==customer2]
    

    df_mergedataset= pd.merge(df_firstcustomerDetails,df_secondcustomerDetails, how='inner',on='RatedToId')
    n=len(df_mergedataset)

    if n==0 : return 0
    
    sum1=sum(df_mergedataset['RatingValue_x'])
    sum2=sum(df_mergedataset['RatingValue_y'])
    
    sum1_square = sum(pow(df_mergedataset['RatingValue_x'],2))
    sum2_square = sum(pow(df_mergedataset['RatingValue_y'],2))
    
    
    product_sum = sum(df_mergedataset['RatingValue_x'] * df_mergedataset['RatingValue_y'])
    #print(customer1)
    
    #calculating the pearson_score
 
    numerator = product_sum - (sum1 * sum2/n )
    denominator = sqrt((sum1_square-pow(sum1,2)/n) * (sum2_square - pow(sum2,2)/n))
    if denominator==0: return 0
  
    finalscore= numerator/denominator
    return finalscore

#This is the method that processes the pearson correlation score and normalize the data and returns the executive Ids
def getTopExecutiveList(customer, similarity=pearson_score):
   
    totalRatings,sumofWeight_AllCustomers= {},{}
    
    #data of the particular customer based on Phone number
    
    data_person= ratingReport.loc[ratingReport.CustomerEmail==customer]
    ranking=[]
    for othercustomerPhoneNumber in list(set(ratingReport.loc[ratingReport['CustomerEmail']!=customer]['CustomerEmail'])): 
             
        # Getting Similarity with othercustomerPhoneNumber
        similaritydataResult=similarity(customer,othercustomerPhoneNumber)
        
        # Ignores Score of Zero or Negative correlation         
        if similaritydataResult<=0: continue    

        df_otherCustomer=ratingReport.loc[ratingReport.CustomerEmail==othercustomerPhoneNumber]
        
        #executive not rated 
        executiveNotRated=df_otherCustomer[~df_otherCustomer.isin(data_person).all(1)] 
        
        for executiveid,rating in (np.array(executiveNotRated[['RatedToId','RatingValue']])):
            
            totalRatings.setdefault(executiveid,0) 
            totalRatings[executiveid]+=rating*similaritydataResult   
            
            #Sum of Similarities
            sumofWeight_AllCustomers.setdefault(executiveid,0)
            sumofWeight_AllCustomers[executiveid]+=similaritydataResult


        #Normalization of list
        ranking=[(t/sumofWeight_AllCustomers[item],item) for item,t in totalRatings.items()]
        ranking.sort()
        ranking.reverse()
    return([recommend_item for recommend_item in  ranking] )

# Display of the executive list data #
def getTopExecutiveListML(customerPhoneNumber,nearbyRadius,pickUpLatitude,pickUpLongitude):
    executivePredictionWithRatingByCustomer={}
    executivePredictionwithNoDataFound={}
    executivePredictionWithRating = getTopExecutiveList(customerPhoneNumber)
    
    for rating,executiveDetail in executivePredictionWithRating:
       executivePredictionWithRatingByCustomer[executiveDetail] = rating

    print(executivePredictionWithRating)
    executivePrediction = [recommend_item for score,recommend_item in  executivePredictionWithRating]

    print('')
    print('--------------------------------------------')
    print('Customer Email/Phone Number : ',customerPhoneNumber)
    print('--------------------------------------------')
    global executiveDataFromML
    executiveDataFromML =[]
    global otherExecutiveData
    otherExecutiveData=[]
    global executiveData
    executiveData={}
    global finalExecutiveData


    data_person= ratingReport.loc[ratingReport.CustomerEmail==customerPhoneNumber][['RatingValue','RatedToId']]

    for index,executiveDetail in data_person.iterrows():
        executivePredictionWithRatingByCustomer[executiveDetail['RatedToId']] = executiveDetail['RatingValue']   
    finalExecutiveData = sorted(executivePredictionWithRatingByCustomer.items(), key=lambda x: x[1],reverse=True)    
    
    print('--------------------------------------------')
    print('List of available executive IDs : ')
    print(executivePredictionWithRatingByCustomer)
    if executivePrediction == []:
        executivePrediction = executiveDetails
        finalExecutiveDatawithNoDataFound = list(executivePrediction.PartyRoleId)       
        for executiveId in  finalExecutiveDatawithNoDataFound:
           executivePredictionwithNoDataFound[executiveId] = 0
           getnearByLocationExecutive(executiveId,nearbyRadius)     
           executivePredictionWithRatingByCustomer ={**executivePredictionwithNoDataFound, **executivePredictionWithRatingByCustomer}  

        finalExecutiveDatawithNoDataFound = sorted(executivePredictionWithRatingByCustomer.items(), key=lambda x: x[1],reverse=True)
        print("---------No data Found-------------")       
        print('')
        print(finalExecutiveDatawithNoDataFound)
        print('')

    else:
        executiveLatitudeandLongitude =""
        for executiveId,rating in  finalExecutiveData:
          
            getnearByLocationExecutive(executiveId,nearbyRadius)

    print(executiveData)
    executiveData = sorted(executiveData.items(), key=lambda x: x[1])

    print('--------------------------------------------')
    print('Executive ID       Distance (km)',)
    print('--------------------------------------------')
    for  executiveid,distance in executiveData:
         if distance <=  nearbyRadius:
             print(executiveid,'                         ',distance)
             executiveDataFromML.append(executiveid)
    print('--------------------------------------------')
    
    return executiveDataFromML

            
def getnearByLocationExecutive(executiveId,nearbyRadius):   
    data_executive= executiveDetails.loc[executiveDetails.PartyRoleId==executiveId][['PickUpDistanceForExecutive']]
    
    if not data_executive.empty:
        data_executive =data_executive
        distanceBetweenExecutive = data_executive.iloc[0]['PickUpDistanceForExecutive']
        executiveData[executiveId]=distanceBetweenExecutive 
      


if __name__ == '__main__':
    import os
    HOST = os.environ.get('SERVER_HOST', 'localhost')
    try:
        PORT = int(os.environ.get('SERVER_PORT', '5555'))
    except ValueError:
        PORT = 5555
    app.run(HOST, PORT)