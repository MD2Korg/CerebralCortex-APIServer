import boto3
import os
from LoggerUtility import *
from CommonConstants import BotoConstants
from CommonConstants import AWSCredentialsConstants

class BotoUtility:
    @staticmethod
    def getClient(resourceType, accNo, roleType, region=''):
        try:
            # format or role ARN is arn:aws:iam::<ACC_NO>:role/Md2kKinesisStreamProducerRole
            roleArn = "arn:aws:iam::" + str(accNo) + ":" + BotoConstants.ROLE_KEYWORD + "/" + BotoConstants.KINESIS_STREAM_PRODUCER_ROLE_NAME
            LoggerUtility.logInfo('Received role ARN as :' + roleArn)

            sts_client = boto3.client(BotoConstants.BOTO_CLIENT_AWS_STS)
            assumedRoleObject = sts_client.assume_role(
                RoleArn=roleArn,
                RoleSessionName=BotoConstants.KINESIS_PRODUCER_ASSUME_ROLE_SESSION_NAME
            )
            credentials = assumedRoleObject[AWSCredentialsConstants.CREDENTIALS_REFERENCE]
            if region == '':
                botoClient = boto3.client(
                    resourceType,
                    aws_access_key_id=credentials[AWSCredentialsConstants.CREDENTIALS_ACCESS_KEY],
                    aws_secret_access_key=credentials[AWSCredentialsConstants.CREDENTIALS_SECRET_KEY],
                    aws_session_token=credentials[AWSCredentialsConstants.CREDENTIALS_SESSION_TOKEN]
                )
            else:
                botoClient = boto3.client(
                    resourceType,
                    aws_access_key_id=credentials[AWSCredentialsConstants.CREDENTIALS_ACCESS_KEY],
                    aws_secret_access_key=credentials[AWSCredentialsConstants.CREDENTIALS_SECRET_KEY],
                    aws_session_token=credentials[AWSCredentialsConstants.CREDENTIALS_SESSION_TOKEN],
                    region_name=region
                )
        except Exception as e:
            LoggerUtility.logWarning(e)
            LoggerUtility.logWarning('Trying to create client object using default values.')
            if region == '':
                botoClient = boto3.session.Session(profile_name='unimemphis').client(resourceType)
            else:
                botoClient = boto3.session.Session(profile_name='unimemphis').client(resourceType, region_name=region)
            LoggerUtility.logInfo('Created client object successfully using default values.')

        return botoClient

    # @staticmethod
    # def getResource(resourceType, accNo, roleType, region=''):
    #     try:
    #         moduleName = os.environ[ManagedCloudConstants.MNC_MODULE_NAME_ENV_VAR] + "_" + roleType + "_" + ROLE_KEYWORD
    #         roleArn = "arn:aws:iam::" + str(accNo) + ":" + ROLE_KEYWORD + "/" + moduleName
    #         sts_client = boto3.client(BotoConstants.BOTO_CLIENT_AWS_STS)
    #         assumedRoleObject = sts_client.assume_role(
    #             RoleArn=roleArn,
    #             RoleSessionName=AWSConfigConstants.CONFIG_ASSUME_ROLE_SESSION_NAME
    #         )
    #         credentials = assumedRoleObject[AWSCredentialsConstants.CREDENTIALS_REFERENCE]
    #         if region == '':
    #             botoResource = boto3.resource(
    #                 resourceType,
    #                 aws_access_key_id=credentials[AWSCredentialsConstants.CREDENTIALS_ACCESS_KEY],
    #                 aws_secret_access_key=credentials[AWSCredentialsConstants.CREDENTIALS_SECRET_KEY],
    #                 aws_session_token=credentials[AWSCredentialsConstants.CREDENTIALS_SESSION_TOKEN]
    #             )
    #         else:
    #             botoResource = boto3.resource(
    #                 resourceType,
    #                 aws_access_key_id=credentials[AWSCredentialsConstants.CREDENTIALS_ACCESS_KEY],
    #                 aws_secret_access_key=credentials[AWSCredentialsConstants.CREDENTIALS_SECRET_KEY],
    #                 aws_session_token=credentials[AWSCredentialsConstants.CREDENTIALS_SESSION_TOKEN],
    #                 region_name=region
    #             )
    #     except Exception as e:
    #         LoggerUtility.logWarning(e)
    #         if region == '':
    #             botoResource = boto3.resource(resourceType)
    #         else:
    #             botoResource = boto3.resource(resourceType, region_name=region)
    #     return botoResource
