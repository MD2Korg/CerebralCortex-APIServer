# before executing this class we need following environment variables to be set
#   awsAccountNumber, logLevel

import os
import json
from CommonConstants import EnvironVariableConstants
from CommonConstants import BotoConstants
from LoggerUtility import LoggerUtility
from BotoUtility import BotoUtility

class KinesisProducer:

    def produceMessage(self, streamMessage, partitionKeyFactor):
        LoggerUtility.setLevel()

        # check for the required variables.
        awsAccountNumber = os.getenv(EnvironVariableConstants.AWS_ACCOUNT_NUMBER, '')
        awsKinesisStreamName = os.getenv(EnvironVariableConstants.AWS_KINESIS_STREAM_NAME, '')
        awsKinesisStreamRegionName = os.getenv(EnvironVariableConstants.AWS_KINESIS_STREAM_REGION, '')
        if(awsAccountNumber == '' or awsKinesisStreamName == '' or awsKinesisStreamRegionName == ''):
            LoggerUtility.logError('Cannot work with empty ' + EnvironVariableConstants.AWS_ACCOUNT_NUMBER + 
                ' or ' + EnvironVariableConstants.AWS_KINESIS_STREAM_NAME + ' value(s).')
            return

        try:
            kinesisClient = BotoUtility.getClient(
                BotoConstants.BOTO_CLIENT_AWS_KINESIS,
                awsAccountNumber,
                BotoConstants.KINESIS_STREAM_PRODUCER_ROLE_NAME,
                awsKinesisStreamRegionName)
            # print(kinesisClient.describe_stream(StreamName='Md2kKinesisStream'))
            kinesisClient.put_record(StreamName=awsKinesisStreamName, 
                Data=json.dumps(streamMessage),
                PartitionKey=str(hash(partitionKeyFactor)))

            LoggerUtility.logInfo("Successfully sent message :" + streamMessage + " to stream :" + awsKinesisStreamName)

        except Exception as e:
            LoggerUtility.logError('Received exception :' + str(e))
            return False

kProd = KinesisProducer()
kProd.produceMessage("This is a demo.", "1")
