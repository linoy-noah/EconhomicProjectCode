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
    console.log(requestBody)
    let response;
    try{
        const userConfigResult = await getUserConfig(requestBody.UserID)
         response = {
            statusCode: 200,
            body: JSON.stringify(userConfigResult),
        };
    }catch(err){
        response = { 
            statusCode: 400,
            body: JSON.stringify({"err":err, "reqBody":requestBody}),
        }
    }
    
    return response;
};

const getUserConfig = async (userID) => {
    try{
        var params = { 
            TableName: UserConfigTableName,
            Key:{
                "UserID":userID
            }, 
        }
        let items = await ddbDocumentClient.get(params).promise()
       let result = {
           ACTime: items.Item.op.AcOp.emptyRoomTime, 
           ACTemp: items.Item.op.AcOp.roomOptimalTemperature, 
           LightTime: items.Item.op.lightsOp.emptyRoomTime, 
           LightLevel:items.Item.op.lightsOp.roomLightsLevel,
           BoilerTemp: items.Item.op.boilerOp.waterOptimalTemperature, 
           WTTime: items.Item.op.waterTapOp.emptyRoomTime
       }
        return result;
    }catch(err){
        return err;
    }
}

