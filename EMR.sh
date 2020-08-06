#!/usr/bin/env bash
# https://aws.amazon.com/about-aws/whats-new/2020/04/amazon-emr-now-available-los-angeles/
# Need a NAT Gateway to use private subnets
private=$(aws ec2 describe-subnets \
  --filters Name=tag:Tier,Values=Public \
  --query 'Subnets[?Tags[?Key==`Name`&&Value==`us-west-2-lax-1a-public-subnet-development`]].SubnetId' \
  --output text)
echo $private

subnets=$(echo $private | sed 's/ /,/g')
echo $subnets

defaultsg=$(aws ec2 describe-security-groups \
  --query 'SecurityGroups[?GroupName==`'bd-security-group-development'`].GroupId' \
  --output text)
echo $defaultsg

AWS_ACCESS_KEY_ID=$(aws ssm get-parameters --names /s3/polyglotDataNerd/admin/AccessKey --query Parameters[0].Value --with-decryption --output text)
AWS_SECRET_ACCESS_KEY=$(aws ssm get-parameters --names /s3/polyglotDataNerd/admin/SecretKey --query Parameters[0].Value --with-decryption --output text)

# shellcheck disable=SC2016
# r5d.xlarge
aws emr create-cluster --profile default --security-configuration EMRSecConfig --applications Name=Spark Name=Flink Name=Livy --tags 'VPC=Private' 'name=Sandbox' 'Environment=development' 'Name=poly-sandbox' --ec2-attributes '{"KeyName":"gerard-bartolome-dev","AdditionalSlaveSecurityGroups":["'"$defaultsg"'"],"InstanceProfile":"admin","SubnetId":"'"$private"'","EmrManagedSlaveSecurityGroup":"'"$defaultsg"'","EmrManagedMasterSecurityGroup":"'"$defaultsg"'","AdditionalMasterSecurityGroups":["'"$defaultsg"'"]}' --release-label emr-5.30.1 --log-uri 's3n://poly-hadoop/' --instance-groups '[{"InstanceCount":1,"EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"SizeInGB":20,"VolumeType":"io1","Iops":1000},"VolumesPerInstance":1}],"EbsOptimized":true},"InstanceGroupType":"MASTER","InstanceType":"c5.xlarge","Name":"Master - 1"},{"InstanceCount":1,"EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"SizeInGB":20,"VolumeType":"io1","Iops":1000},"VolumesPerInstance":1}]},"InstanceGroupType":"CORE","InstanceType":"r5d.xlarge","Name":"Core - 2"}]' --configurations '
[
{"Classification":"spark","Properties":{"authenticate":"true","maximizeResourceAllocation":"true"}},
{"Classification":"emrfs-site",
"Properties": {
    "fs.s3.serverSideEncryptionAlgorithm":"AES256",
    "fs.s3.enableServerSideEncryption":"true",
    "fs.s3a.access.key":"'"$AWS_ACCESS_KEY_ID"'",
    "fs.s3a.secret.key":"'"$AWS_SECRET_ACCESS_KEY"'"
  }
},
{
      "Classification": "flink-conf",
      "Properties": {
        "taskmanager.numberOfTaskSlots":"4",
        "parallelism.default":"6",
        "taskmanager.memory.process.size":"10728m",
        "jobmanager.heap.mb": "10728m",
        "taskmanager.heap.mb": "10728m"
      }
    }
]' --service-role EMR_DefaultRole --enable-debugging --name 'poly-sandbox' --scale-down-behavior TERMINATE_AT_TASK_COMPLETION --region us-west-2
