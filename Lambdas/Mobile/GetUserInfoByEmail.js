var AWS = require("aws-sdk");
const HouseTableName = 'HouseTest'
const UserTableName ='UserTest'
const PowerAndWaterUsageTableName= 'PowerAndWaterUsage'

AWS.config.update({region: 'eu-west-1'});
// Create the Service interface for DynamoDB
var dynamodb = new AWS.DynamoDB();

// Create the Document Client interface for DynamoDB
var ddbDocumentClient = new AWS.DynamoDB.DocumentClient();
exports.handler = async (event) => {
    const requestBody = JSON.parse(event.body);
    try{
        console.log("email",requestBody.userEmail )
        const userResult = await prepareUserData(requestBody.userEmail)
        const response = {
            statusCode: 200,
            body: JSON.stringify({user:userResult}),
        };
         return response;

    }catch(err){
        
        const response = {
            statusCode: 400,
            body: JSON.stringify(err),
        };
         return response;
    }
    
   
};    


async function getUserByEmail(email){
    try{
        var params = { 
            TableName: "UserTest",
            FilterExpression: "userDetails.email = :email",
             ExpressionAttributeValues: { 
                ":email":email
              }
        }
        var result = await ddbDocumentClient.scan(params).promise();
        console.log(" getUserByEmail result",result)
        return result;
    }catch(err){
        return err;
    }
}
const getPowerAndWaterUsageByID = async (id) => {
    console.log("id", id)
     try{
        let params = { 
            TableName: PowerAndWaterUsageTableName,
            Key:{
                "UserID":id
            }, 
        }
        let items = await ddbDocumentClient.get(params).promise()
        console.log(items)
        return items.Item.Devices;
    }catch(err){
        return err;
    }
}

const prepareUserData = async (email) => {
     try{
        let userItem = await getUserByEmail(email);
        let userResult = userItem.Items[0]
        let devices = await getPowerAndWaterUsageByID(userResult.UserID)
        userResult["Expenses"] = devices
        console.log("prepareUserData result", userResult)
        return userResult;
    }catch(err){
        return err;
    }
}