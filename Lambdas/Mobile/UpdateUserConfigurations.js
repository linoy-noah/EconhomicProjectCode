var AWS = require("aws-sdk");

const UserConfigTableName = 'UserConfig'

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
        const updateResult = await updateUserConfig(requestBody)
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


async function updateUserConfig(values){
    console.log(values)
    let params = {
        TableName: UserConfigTableName,
        Key: {
            "UserID":values.userID,
        },
        UpdateExpression: "set op.AcOp.emptyRoomTime = :ACemptyRoomTime, op.AcOp.configBy=:user, op.AcOp.roomOptimalTemperature=:ACroomOptimalTemperature, op.boilerOp.waterOptimalTemperature=:boilerWaterOptimalTemperature, op.boilerOp.cofigBy=:user,op.lightsOp.emptyRoomTime=:lightEmptyRoomTime,op.lightsOp.roomLightsLevel=:roomLightsLevel ,op.lightsOp.configBy=:user, op.waterTapOp.emptyRoomTime=:WTemptyRoomTime",
        ExpressionAttributeValues: {
          ":ACemptyRoomTime":values.values.timeData.AC, 
          ":ACroomOptimalTemperature":values.values.temperatureData.AC,
          ":boilerWaterOptimalTemperature":values.values.temperatureData.Boiler,
          ":lightEmptyRoomTime":values.values.timeData.Light, 
          ":roomLightsLevel":values.values.lightLevel,
          ":WTemptyRoomTime":values.values.timeData.Water, 
          ":user": "user"
          
        },
    };
    try {
        console.log("before try")
        const result = await ddbDocumentClient.update(params).promise()
        console.log("after call")
        console.log("result", result)
        return result;
    } catch (err) {
        console.log(err)
        return err
    }
}
