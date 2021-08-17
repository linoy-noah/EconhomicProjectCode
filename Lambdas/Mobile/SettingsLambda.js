var AWS = require("aws-sdk");

const UserTableName = 'UserTest'

AWS.config.update({
    region: 'eu-west-1'
});
// Create the Service interface for DynamoDB
var dynamodb = new AWS.DynamoDB();
var ddbDocumentClient = new AWS.DynamoDB.DocumentClient();

exports.handler = async (event) => {
    const requestBody = JSON.parse(event.body);
    console.log(requestBody)
    let response;
    try{
        const updateResult = await setUpdateSettings(requestBody)
         response = {
            statusCode: 200,
            body: JSON.stringify({"updateResult":updateResult, "reqBody":requestBody}),
        };
    }catch(err){
        response = { 
            statusCode: 400,
            body: JSON.stringify({"err":err, "reqBody":requestBody}),
        }
    }
    
    return response;
};


async function setUpdateSettings(userConstaints){
    let params = {
        TableName: UserTableName,
        Key: {
            "UserID":userConstaints.UserID,
        },
        UpdateExpression: "set UserConstraints.electricityBudget = :electricityBudget, UserConstraints.numberOfHouseMembers=:numberOfHouseMembers, UserConstraints.waterBudget=:waterBudget",
        ExpressionAttributeValues: {
            ":electricityBudget":parseInt(userConstaints.electricityBudget),
            ":waterBudget":parseInt(userConstaints.waterBudget),
            ":numberOfHouseMembers":parseInt(userConstaints.numberOfHouseMembers)
        },
    };
    try {
        const result = await ddbDocumentClient.update(params).promise()
        return result;
    } catch (err) {
        console.log("setUpdateSettings err", err)
        return err
    }
}
