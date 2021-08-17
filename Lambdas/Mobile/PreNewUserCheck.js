var AWS = require("aws-sdk");
const HouseTableName = 'HouseTest'
const UserTableName ='UserTest'

AWS.config.update({region: 'eu-west-1'});
// Create the Service interface for DynamoDB
var dynamodb = new AWS.DynamoDB();

// Create the Document Client interface for DynamoDB
var ddbDocumentClient = new AWS.DynamoDB.DocumentClient();
exports.handler = async (event) => {
    const requestBody = JSON.parse(event.body);
    try{
        const houseResult = await getHouseByID(requestBody.houseID)
        const userResult = await getUserByHouseID(requestBody.houseID)
        
        const response = {
            statusCode: 200,
            body: JSON.stringify({house:houseResult, user:userResult}),
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


async function getHouseByID(houseID){
    try{
        var params = { 
            TableName: HouseTableName,
            Key:{
                "HouseID":{"S":houseID}
            }
        }
        var result = await dynamodb.getItem(params).promise()
        return result;
    }catch(err){
        return err;
    }
}

async function getUserByHouseID(houseID){
    try{
        var params = { 
            TableName: "UserTest",
            FilterExpression: "HouseID = :houseID",
             ExpressionAttributeValues: { 
                ":houseID":{"S": houseID}
              }
        }
        var result = await dynamodb.scan(params).promise();
        return result;
    }catch(err){
        return err;
    }
}