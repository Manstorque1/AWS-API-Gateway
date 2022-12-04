import boto3
import json
import logging
from custom_encoder import CustomEncoder

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodbTableName = 'product-inventory'
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(dynamodbTableName)


getMethod = 'GET'
postMethod = 'POST'
patchMethod = 'PATCH'
deleteMethod = 'DELETE'

healthPath = '/health'
productPath = '/product'
productListPath = '/products'



def lambda_handler(event, context):
    logger.info(event)

    httpMethod = event['httpMethod']
    path = event['path']

    if httpMethod == getMethod and path == healthPath:
        response = getResponse(200)

    elif httpMethod == getMethod and path == productPath:
        response = getProduct(event['queryStringParameters']['productId'])

    elif httpMethod == getMethod and path == productListPath:
        response = getProductList()

    elif httpMethod == postMethod and path == productPath:
        response = saveProduct(json.loads(event['body']))

    elif httpMethod == patchMethod and path == productPath:
        requestBody = json.loads(event['body'])
        response = updateProduct(requestBody['productId'], requestBody['updateKey'], requestBody['updateValue'])

    elif httpMethod == deleteMethod and path == productPath:
        requestBody = json.loads(event['body'])
        response = deleteProduct(requestBody['productId'])

    else:
        response = getResponse(404, 'Not Found')
    
    return response




def getProduct(productId):
    try:
        response = table.get_item(
            Key = {
                'productId' : productId
            }
        )
        if 'Item' in response:
            return getResponse(200, response['Item'])
        else:
            return getResponse(404, {'Message' : 'ProductId : %s not found' %productId})
    except:
        logger.exception('Something went wrong!!!')





def getProductList():
    try:
        response = table.scan()
        result = response['Items']

        while 'LastEvaluatedKey' in response:
            response = table.scan(ExclusiveStartKey = response['LastEvaluatedKey'])
            result.extend(response['Item'])

        body = {
            'products' : response
        }

        return getResponse(200, body)

    except:
        logger.exception('Something went wrong!!!')
    




def saveProduct(requestBody):
    try:
        table.put_item(Item=requestBody)
        body = {
            'Operation' : 'SAVE',
            'Message' : 'SUCCESS',
            'Item' : requestBody
        }
        return getResponse(200, body)
    except:
        logger.exception('Something went wrong!!!')





def deleteProduct(productId):
    try:
        response = table.delete_item(
            Key = {
                'productId' : productId
            },
            ReturnValues = 'ALL_OLD'
        )

        body = {
            'Operation' : 'SAVE',
            'Message' : 'SUCCESS',
            'deletedItem' : response
        }
        return getResponse(200, body)
    except:
        logger.exception('Something went wrong!!!')





def updateProduct(productId, updateKey, updateValue):
    try:
        response = table.update_item(
            Key = {
                'productId' : productId
            },
            UpdateExpression = 'set %s = :val' %updateKey,
            ExpressionAttributeValues = {
                ':val' : updateValue
            },
            ReturnValues = 'UPDATED_NEW'
        )

        body = {
            'Operation' : 'UPDATE',
            'Message' : 'SUCCESS',
            'UpdatedAttributes' : response
        }
        return getResponse(200, body)
    
    except:
        logger.exception('Something went wrong!!!')





def getResponse(statusCode, body=None):

    response = {
        'statusCode' : statusCode,
        'headers' : {
            'Content-Type' : 'application/json',
            'Access-Control-Allow-Origin' : '*'
        }
    }

    if body is not None:
        response['body'] = json.dumps(body, cls=CustomEncoder)
    return response
