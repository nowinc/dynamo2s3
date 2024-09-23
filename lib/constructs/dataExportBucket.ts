import { Construct } from 'constructs';
import * as cdk from 'aws-cdk-lib';
import {aws_s3 as s3, aws_iam as iam} from "aws-cdk-lib"
import { ExportFormat } from '../constants/exportFormat';

export interface BucketProps {
  name: string;
  region: string;
  account: string;
  sourceDdbTablename: string;
  deploymentAlias: string;
  prefix?: string;
  exportFormat: ExportFormat
}

export class DataExportBucket extends Construct {

  bucket: s3.IBucket;
  prefix?: string;
  exportFormat: ExportFormat;

  constructor(scope: Construct, id: string, props: BucketProps) {
    super(scope, id);

    let bucketName = props.name;

    if (!bucketName || bucketName === "") {

      bucketName = `${props.deploymentAlias}-data-export`;

      const serverAccessLogBucket = new s3.Bucket(this, `${id}-resource-server-access-log-bucket`, {
        bucketName: `${bucketName}-server-access-logs`,
        autoDeleteObjects: false,
        removalPolicy: cdk.RemovalPolicy.RETAIN,
      });
      this.onlyHttpsRequestsAllowed(serverAccessLogBucket);
      
      this.bucket = new s3.Bucket(this, `${id}-resource`, {
        bucketName: bucketName,
        encryption: s3.BucketEncryption.KMS_MANAGED,
        autoDeleteObjects: false,
        removalPolicy: cdk.RemovalPolicy.RETAIN,
        serverAccessLogsBucket: serverAccessLogBucket,
        
        // more props to be set here as needed
      });
      this.onlyHttpsRequestsAllowed(this.bucket);
    }
    else {
      this.bucket = s3.Bucket.fromBucketName(this, `${id}-resource`, bucketName);
    }

    this.prefix = props.prefix;
    this.exportFormat = props.exportFormat;

    new cdk.CfnOutput(this, `${props.deploymentAlias}-data-export-bucket`, {
      value: this.bucket.bucketName,
      exportName: `${props.deploymentAlias}-data-export-bucket`,
      description: 'Data export bucket'
    });

    if (this.prefix !== undefined && this.prefix !== "") {
      new cdk.CfnOutput(this, `${props.deploymentAlias}-data-export-bucket-prefix`, {
        value: this.prefix,
        exportName: `${props.deploymentAlias}-data-export-bucket-prefix`,
        description: 'Data export bucket prefix'
      });
    }
  }

  private onlyHttpsRequestsAllowed(bucket: s3.IBucket) {
    bucket.addToResourcePolicy(
      new iam.PolicyStatement({
        actions: ['s3:*'],
        effect: iam.Effect.DENY,
        principals: [new iam.AnyPrincipal()],
        resources: [bucket.bucketArn, bucket.bucketArn + "/*"],
        conditions: {
          Bool: {
            'aws:SecureTransport': 'false'
          },
        },
      })
    );
  }

  hasPrefix() : boolean {
    return this.prefix !== undefined && this.prefix !== "";
  }

  public getExecuteExportParameters(sourceDynamoDbTableArn: string): any {
    if (!this.hasPrefix()) {
      return {
          S3Bucket: this.bucket.bucketName,
          TableArn: sourceDynamoDbTableArn,
          ExportFormat: ExportFormat[this.exportFormat]
      };
    } else {
      return {
          S3Bucket: this.bucket.bucketName,
          'S3Prefix.$': `$.getNextIncrementalExportTimeOutput.Payload.body.dateS3BucketPrefix`,
          TableArn: sourceDynamoDbTableArn,
          ExportFormat: ExportFormat[this.exportFormat]
      };
    }
  }

  public getExecuteFullExportParameters(sourceDynamoDbTableArn: string): any {
    if (!this.hasPrefix()) {
      return {
          S3Bucket: this.bucket.bucketName,
          TableArn: sourceDynamoDbTableArn,
          ExportFormat: ExportFormat[this.exportFormat]
      };
    } else {
      return {
          S3Bucket: this.bucket.bucketName,
          S3Prefix: this.prefix,
          TableArn: sourceDynamoDbTableArn,
          ExportFormat: ExportFormat[this.exportFormat]
      };
    }
  }
}
