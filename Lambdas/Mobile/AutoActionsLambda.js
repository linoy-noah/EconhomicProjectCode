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
    let response;
    try{
        const updateResult = await setUpdateActions(requestBody)
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

async function setUpdateActions(userActions){
    let params = {
        TableName: UserTableName,
        Key: {
            "UserID":userActions.UserID,
        },
        UpdateExpression: "set AutomaticActions.AirConditioner = :AirConditioner ,AutomaticActions.Light=:Light, AutomaticActions.Boiler=:Boiler, AutomaticActions.WaterTap=:WaterTap ",
        ExpressionAttributeValues: {
            ":AirConditioner":userActions.AirConditioner,
            ":Light":userActions.Light,
            ":Boiler":userActions.Boiler, 
            ":WaterTap":userActions.WaterTap
            
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
