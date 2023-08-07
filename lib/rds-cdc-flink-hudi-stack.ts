import * as cdk from 'aws-cdk-lib';
import {CustomResource, Duration, RemovalPolicy, ResolutionTypeHint, SecretValue} from 'aws-cdk-lib';
import {Construct} from 'constructs';
// import * as sqs from 'aws-cdk-lib/aws-sqs';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as assets from 'aws-cdk-lib/aws-s3-assets'
import * as path from "path";
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import {SubnetType} from 'aws-cdk-lib/aws-ec2';
import {readFileSync} from "fs";
import * as rds from 'aws-cdk-lib/aws-rds';
import * as sm from "aws-cdk-lib/aws-secretsmanager";
import {Secret} from "aws-cdk-lib/aws-secretsmanager";
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as glue from 'aws-cdk-lib/aws-glue';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as kda from 'aws-cdk-lib/aws-kinesisanalyticsv2';
import {custom_resources} from "aws-cdk-lib";
import {start} from "repl";
import * as events from 'aws-cdk-lib/aws-events'
import {Rule} from "aws-cdk-lib/aws-events";
import * as targets from 'aws-cdk-lib/aws-events-targets';
import * as lakeformation from 'aws-cdk-lib/aws-lakeformation';

export class RdsCdcFlinkHudiStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    //Jar Connectors
    const hudiFlinkJar = new assets.Asset(this, 'hudiFlinkJar', {
      path: path.join(__dirname, '../connectors/hudi-flink1.15-bundle-0.13.1.jar'),
    });
    const hiveExecJar = new assets.Asset(this, 'hiveExecJar', {
      path: path.join(__dirname, '../connectors/hive-exec-2.3.1.jar'),
    });
    const hiveCommonJar = new assets.Asset(this, 'hiveCommonJar', {
      path: path.join(__dirname, '../connectors/hive-common-2.3.1.jar'),
    });
    const hadoopMapReduceJar = new assets.Asset(this, 'hadoopMapReduceJar', {
      path: path.join(__dirname, '../connectors/hadoop-mapreduce-client-core-3.3.6.jar'),
    });
    const flinkSQLPostgresJar = new assets.Asset(this, 'flinkSQLPostgresJar', {
      path: path.join(__dirname, '../connectors/flink-sql-connector-postgres-cdc-2.4.1.jar'),
    });
    const dataNucleusJar = new assets.Asset(this, 'dataNucleusJar', {
      path: path.join(__dirname, '../connectors/datanucleus-core-6.0.4.jar'),
    });
    const flinkS3FsJar = new assets.Asset(this, 'flinkS3FsJar', {
      path: path.join(__dirname, '../connectors/flink-s3-fs-hadoop-1.17.1.jar'),
    });

    // VPC
    const vpc = new ec2.Vpc(this, 'VPC', {
      enableDnsHostnames: true,
      enableDnsSupport: true,
      maxAzs: 3,
      natGateways: 1,
      subnetConfiguration: [
        {
          cidrMask: 24,
          name: 'public-subnet-1',
          subnetType: ec2.SubnetType.PUBLIC,
        },
        {
          cidrMask: 24,
          name: 'private-subnet',
          subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS,
        }
      ],
      gatewayEndpoints: {
        S3: {
          service: ec2.GatewayVpcEndpointAwsService.S3
        }
      }
    });


    const s3Bucket = new s3.Bucket(this, 'Bucket', {
      bucketName : 'rds-cdc-flink-hudi-'+this.account+'-'+this.region,
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      encryption: s3.BucketEncryption.S3_MANAGED,
      enforceSSL: true,
      versioned: true,
      removalPolicy: RemovalPolicy.DESTROY,
      autoDeleteObjects: true

    });

    //security group for RDS
    const rdsSG = new ec2.SecurityGroup(this, 'rdsSG', {
      vpc: vpc,
      allowAllOutbound: true,
      description: 'RDS Security Group'
    });

    rdsSG.connections.allowInternally(ec2.Port.allTraffic(), 'Allow all traffic between hosts having the same security group');

    //security group for EC2
    const ec2SG = new ec2.SecurityGroup(this, 'ec2SG', {
      vpc: vpc,
      allowAllOutbound: true,
      description: 'ec2 Security Group'
    });

    rdsSG.addIngressRule(ec2SG,ec2.Port.tcp(5432))

    //rdsSG.addIngressRule(ec2.Peer.ipv4('0.0.0.0/0'),ec2.Port.tcp(5432));

    rdsSG.connections.allowInternally(ec2.Port.allTraffic(), 'Allow all traffic between hosts having the same security group');

    ec2SG.connections.allowInternally(ec2.Port.allTraffic(), 'Allow all traffic between hosts having the same security group');

    const glueEndpoint = new ec2.InterfaceVpcEndpoint(this, 'glue endpoint', {
      vpc,
      service: new ec2.InterfaceVpcEndpointService('com.amazonaws.'+this.region+'.glue'),
      securityGroups: [rdsSG,ec2SG],
      privateDnsEnabled: true,
    }
  );


    const dbParameterGroup = new rds.ParameterGroup(this, 'ParameterGroup', {
      engine: rds.DatabaseInstanceEngine.postgres({version: rds.PostgresEngineVersion.VER_12_12}),
      parameters: {
        "rds.logical_replication" : '1',
        "wal_sender_timeout": '0',
        "max_wal_senders": '20',
        "max_replication_slots": '50'
      },

    });

    const dbInstance = new rds.DatabaseInstance(this, 'RDS', {
      engine: rds.DatabaseInstanceEngine.postgres({version: rds.PostgresEngineVersion.VER_12_12}),
      vpc: vpc,
      vpcSubnets: {subnetType: ec2.SubnetType.PUBLIC},
      publiclyAccessible: true,
      instanceType: ec2.InstanceType.of(ec2.InstanceClass.BURSTABLE3, ec2.InstanceSize.XLARGE2),
      credentials: rds.Credentials.fromPassword("adminuser",SecretValue.unsafePlainText('admin123')),
      maxAllocatedStorage: 800,
      allocatedStorage: 500,
      port: 5432,
      storageType: rds.StorageType.GP3,
      backupRetention: Duration.days(0),
      databaseName: 'sportstickets',
      parameterGroup: dbParameterGroup,
      securityGroups: [rdsSG],
      });

    dbInstance.node.addDependency(vpc);


    const ec2LabInstance = new ec2.Instance(this, 'targetInstance', {
      vpc: vpc,
      vpcSubnets: {
        subnetType: SubnetType.PUBLIC
      },
      instanceType: ec2.InstanceType.of(ec2.InstanceClass.BURSTABLE2, ec2.InstanceSize.MEDIUM),
      machineImage: new ec2.AmazonLinuxImage({ generation: ec2.AmazonLinuxGeneration.AMAZON_LINUX_2 }),
      securityGroup: ec2SG,
      userDataCausesReplacement: true,
    });

    const userDataScript = readFileSync('./scripts/userdata.sh','utf8').replace('${!ENDPOINT}',dbInstance.dbInstanceEndpointAddress);

    ec2LabInstance.addUserData(userDataScript);

    ec2LabInstance.node.addDependency(dbInstance);

    const cdcFunction = new lambda.Function(this, "cdc Lambda", {
      code: lambda.Code.fromAsset('./scripts/cdclambda/'),
      handler: "index.handler",
      memorySize: 128,
      runtime: lambda.Runtime.NODEJS_18_X,
      timeout: Duration.seconds(300),
      vpc: vpc,
      vpcSubnets: {subnetType : ec2.SubnetType.PRIVATE_WITH_EGRESS},
      securityGroups: [ec2SG],
      environment: {
        "HOST": dbInstance.dbInstanceEndpointAddress,
        "PASSWORD": "admin123"
      }
    });



    cdcFunction.node.addDependency(ec2LabInstance);


    const glueKDADatabase = new glue.CfnDatabase(this, "gluedatabase", {
      catalogId: this.account.toString(),
      databaseInput: {
        name: 'gluedatabase'
      }
    });

    const hudiDatabase = new glue.CfnDatabase(this, "hudidatabase", {
      catalogId: this.account.toString(),
      databaseInput: {
        name: 'hudidatabase'
      }
    });

    // our KDA app needs access to perform VPC actions
    const accessVPCPolicy = new iam.PolicyDocument({
      statements: [
        new iam.PolicyStatement({
          resources: ['*'],
          actions: ['ec2:DeleteNetworkInterface',
            'ec2:DescribeDhcpOptions',
            'ec2:DescribeSecurityGroups',
            'ec2:CreateNetworkInterface',
            'ec2:DescribeNetworkInterfaces',
            'ec2:CreateNetworkInterfacePermission',
            'ec2:DescribeVpcs',
            'ec2:DescribeSubnets'],
        }),
      ],
    });

    // our KDA app needs to be able to log
    const accessCWLogsPolicy = new iam.PolicyDocument({
      statements: [
        new iam.PolicyStatement({
          resources: ['*'],
          actions: ['logs:*','cloudwatch:*'],
        }),
      ],
    });

    // our KDA app needs access to describe kinesisanalytics
    const kdaAccessPolicy = new iam.PolicyDocument({
      statements: [
        new iam.PolicyStatement({
          resources: ['*'],
          actions: ['kinesisanalytics:*']
        }),
      ],
    });

    // our KDA app needs access to access glue db
    const glueAccessPolicy = new iam.PolicyDocument({
      statements: [
        new iam.PolicyStatement({
          resources: ['*'],
          actions: ['glue:*']
        }),
      ],
    });

    const kdaRole = new iam.Role(this, 'KDA Role', {
      assumedBy: new iam.ServicePrincipal('kinesisanalytics.amazonaws.com'),
      managedPolicies: [iam.ManagedPolicy.fromAwsManagedPolicyName('AmazonS3FullAccess')],
      inlinePolicies: {
        AccessCWLogsPolicy: accessCWLogsPolicy,
        AccessVPCPolicy: accessVPCPolicy,
        KDAAccessPolicy: kdaAccessPolicy,
        GlueAccessPolicy: glueAccessPolicy,
      },
    });

    const cfnPermissions = new lakeformation.CfnPermissions(this,'Describe Database permissions', {
      dataLakePrincipal: {
        dataLakePrincipalIdentifier: kdaRole.roleArn
      },
      resource: {
        databaseResource: {
          catalogId: this.account,
          name: 'gluedatabase'
        }
      },
      permissions: ['ALL']
    });

    cfnPermissions.node.addDependency(glueKDADatabase);


    const kdaStudio = new kda.CfnApplication(this, 'CDC Hudi Studio', {
      applicationName: 'cdc-hudi-studio',
      applicationMode: 'INTERACTIVE',
      runtimeEnvironment: 'ZEPPELIN-FLINK-3_0',
      serviceExecutionRole: kdaRole.roleArn,
      applicationConfiguration: {
        vpcConfigurations: [{
          securityGroupIds: [rdsSG.securityGroupId,ec2SG.securityGroupId],
          subnetIds: vpc.selectSubnets({subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS,}).subnetIds
        }],
        zeppelinApplicationConfiguration: {
          monitoringConfiguration: {
            logLevel: "ERROR"
          },
          catalogConfiguration: {
            glueDataCatalogConfiguration: {
              databaseArn: 'arn:aws:glue:'+this.region+':'+this.account+':database/gluedatabase'
            }
          },
          customArtifactsConfiguration: [{
            artifactType: 'DEPENDENCY_JAR',
            s3ContentLocation: {
              bucketArn: 'arn:aws:s3:::'+hudiFlinkJar.s3BucketName,
              fileKey: hudiFlinkJar.s3ObjectKey
            }
          },
            {
              artifactType: 'DEPENDENCY_JAR',
              s3ContentLocation: {
                bucketArn: 'arn:aws:s3:::'+hiveExecJar.s3BucketName,
                fileKey: hiveExecJar.s3ObjectKey
              }
            },
            {
              artifactType: 'DEPENDENCY_JAR',
              s3ContentLocation: {
                bucketArn: 'arn:aws:s3:::'+hiveCommonJar.s3BucketName,
                fileKey: hiveCommonJar.s3ObjectKey
              }
            },
            {
              artifactType: 'DEPENDENCY_JAR',
              s3ContentLocation: {
                bucketArn: 'arn:aws:s3:::'+hadoopMapReduceJar.s3BucketName,
                fileKey: hadoopMapReduceJar.s3ObjectKey
              }
            },
            {
              artifactType: 'DEPENDENCY_JAR',
              s3ContentLocation: {
                bucketArn: 'arn:aws:s3:::'+flinkS3FsJar.s3BucketName,
                fileKey: flinkS3FsJar.s3ObjectKey
              }
            },
            {
              artifactType: 'DEPENDENCY_JAR',
              s3ContentLocation: {
                bucketArn: 'arn:aws:s3:::'+flinkSQLPostgresJar.s3BucketName,
                fileKey: flinkSQLPostgresJar.s3ObjectKey
              }
            },
            {
              artifactType: 'DEPENDENCY_JAR',
              s3ContentLocation: {
                bucketArn: 'arn:aws:s3:::'+dataNucleusJar.s3BucketName,
                fileKey: dataNucleusJar.s3ObjectKey
              }
            },
          ]
        },
        flinkApplicationConfiguration: {
          parallelismConfiguration: {
            parallelism: 8,
            configurationType: 'CUSTOM'
          },
          monitoringConfiguration: {
            configurationType: 'DEFAULT'
          },
        }
      }
    });

    const startKDAFunction = new lambda.Function(this, "Start KDA Function", {
      code: lambda.Code.fromInline(`
import cfnresponse
import json
import logging
import signal
import boto3
import time

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

timeout_seconds = 550
poll_interval_seconds = 1


def handler(event, context):
    # Setup alarm for remaining runtime minus a second
    signal.alarm(timeout_seconds)
    try:
        LOGGER.info('Request Event: %s', event)
        LOGGER.info('Request Context: %s', context)
        if event['RequestType'] == 'Create':
            start_app(event['ResourceProperties']['AppName'])
            cfnresponse.send(event, context, cfnresponse.SUCCESS, {
                "Message": "Resource created"})
        elif event['RequestType'] == 'Update':
            start_app(event['ResourceProperties']['AppName'])
            cfnresponse.send(event, context, cfnresponse.SUCCESS, {
                "Message": "Resource updated"})
        elif event['RequestType'] == 'Delete':
            cfnresponse.send(event, context, cfnresponse.SUCCESS, {
                "Message": "Resource deleted"})
        else:
            err = f"Unknown RequestType: {event['RequestType']}"
            LOGGER.error(err)
            cfnresponse.send(
                event, context, cfnresponse.FAILED, {"Message": err})
    except Exception as e:
        LOGGER.error("Failed %s", e)
        cfnresponse.send(event, context, cfnresponse.FAILED,
                         {"Message": str(e)})


def start_app(appName):
    client = boto3.client('kinesisanalyticsv2')
    desc_response = client.describe_application(ApplicationName=appName)
    status = desc_response['ApplicationDetail']['ApplicationStatus']
    if status == "READY":
        # We assume that after a successful invocation of this API
        # application would not be in READY state.
        client.start_application(ApplicationName=appName)
    while (True):
        desc_response = client.describe_application(ApplicationName=appName)
        status = desc_response['ApplicationDetail']['ApplicationStatus']
        if status != "STARTING":
            if status != "RUNNING":
                raise Exception(f"Unable to start the app in state: {status}")
            LOGGER.info(f"Application status changed: {status}")
            break
        else:
            time.sleep(poll_interval_seconds)


def timeout_handler(_signal, _frame):
    '''Handle SIGALRM'''
    raise Exception('Operation timed out')


signal.signal(signal.SIGALRM, timeout_handler)`),
      handler: "index.handler",
      memorySize: 1024,
      runtime: lambda.Runtime.PYTHON_3_9,
      initialPolicy: [
        new iam.PolicyStatement(
            {
              actions: ['kinesisanalytics:DescribeApplication',
                'kinesisanalytics:StartApplication',],
              resources: ['*']
            })
      ],
      timeout: Duration.seconds(600),
    });

    const resource = new CustomResource(this, 'AppStartLambdaResource', {
      serviceToken: startKDAFunction.functionArn,
      properties: {
        AppName: kdaStudio.applicationName
      }
    });

    resource.node.addDependency(startKDAFunction);
    resource.node.addDependency(kdaStudio);


    cdcFunction.node.addDependency(ec2LabInstance);


    const ticket_view_hudi = new glue.CfnTable(this, 'ticket_view_hudi', {
      catalogId: this.account,
      databaseName: 'hudidatabase',
      tableInput: {
        name: 'ticket_view',
        owner: 'hadoop',
        retention: 0,
        tableType: 'EXTERNAL',
        parameters: {
          'EXTERNAL':'TRUE'
        },
        storageDescriptor: {
          columns: [{
            name: '_hoodie_commit_time',
            type: 'string'
          },
            {
              name: '_hoodie_commit_seqno',
              type: 'string'
            },
            {
              name: '_hoodie_record_key',
              type: 'string'
            },
            {
              name: '_hoodie_partition_path',
              type: 'string'
            },
            {
              name: '_hoodie_file_name',
              type: 'string'
            },
            {
              name: 'full_name',
              type: 'string'
            },
            {
              name: 'id',
              type: 'string'
            },
            {
              name: 'ticket_price',
              type: 'float'
            },
            {
              name: 'transaction_date_time',
              type: 'string'
            }
          ],
          numberOfBuckets: -1,
          inputFormat: 'org.apache.hudi.hadoop.realtime.HoodieParquetRealtimeInputFormat',
          outputFormat: 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
          serdeInfo: {
            serializationLibrary: 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe',
            parameters: {
              'serialization.format':1
            }
          },
          location: 's3://'+s3Bucket.bucketName+'/hudi/ticketview/'
        }
      }
    });

    const cdcRule = new events.Rule(this, 'ScheduledLambda', {
      schedule: events.Schedule.cron({minute: '0/1'})});

    cdcRule.addTarget(new targets.LambdaFunction(cdcFunction, {
      event: events.RuleTargetInput.fromObject({message:"Triggering CDC"})}
    ));

    targets.addLambdaPermission(cdcRule,cdcFunction);

    new cdk.CfnOutput(this, 'RDSInstanceEndpoint', {
      value: dbInstance.dbInstanceEndpointAddress,
      description: 'DMS Instance Endpoint'
    });

    new cdk.CfnOutput(this, 'S3DataLakeBucketName', {
      value: s3Bucket.bucketName,
      description: 'S3 Bucket used for Data Lake. Please update in notebook where you see <S3-Bucket>'
    });

  }
}
