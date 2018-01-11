# file containing all the class defintions used for constant declarations.
import abc

class AbstractConstants:
    __metaclass__ = abc.ABCMeta

    def __setattr__(self, attr, value):
        if hasattr(self, attr):
            raise Exception("Attempting to alter read-only value")
        self.__dict__[attr] = value

class EnvironVariableConstants(AbstractConstants):
    AWS_KINESIS_STREAM_NAME = "awsKinesisStreamName"
    AWS_KINESIS_STREAM_REGION = "awsKinesisStreamRegionName"
    AWS_ACCOUNT_NUMBER = "awsAccountNumber"

class BotoConstants:
    BOTO_CLIENT_AWS_STS = "sts"
    BOTO_CLIENT_AWS_KINESIS = "kinesis"
    KINESIS_PRODUCER_ASSUME_ROLE_SESSION_NAME = "KinesisProducerAssumeRoleSession"
    ROLE_KEYWORD = "role"
    KINESIS_STREAM_PRODUCER_ROLE_NAME = "Md2kKinesisStreamProducerRole" # this value is used in the CFT and should be kept same or else the code would break

class AWSCredentialsConstants:
    CREDENTIALS_REFERENCE = "Credentials"
    CREDENTIALS_ACCESS_KEY = "AccessKeyId"
    CREDENTIALS_SECRET_KEY = "SecretAccessKey"
    CREDENTIALS_SESSION_TOKEN = "SessionToken"

    def __setattr__(self, attr, value):
        if hasattr(self, attr):
            raise Exception("Attempting to alter read-only value")

        self.__dict__[attr] = value

class LoggerConstants:
    LOGGER_NAME = "Md2kLogger"
    LOGGER_DEFAULT_LOG_LEVEL = "INFO"
    LOGGER_LOG_LEVEL_ENV_VAR = "logLevel"