import boto3
import collections
import datetime

ec = boto3.client('ec2')

def lambda_handler(event, context):
    reservations = ec.describe_instances(
        Filters=[
            {'Name': 'tag-key', 'Values': ['Backup']},
        ]
    ).get(
        'Reservations', []
    )
    
    
    instances = sum(
        [
            [i for i in r['Instances']]
            for r in reservations
        ], [])
        
   

    print "Found %d instances that have backup tag" % len(instances)

    to_tag = collections.defaultdict(list)

    for instance in instances:
        BackupTag = [(t.get('Value')) for t in instance['Tags']
        if t['Key'] == 'Backup'][0]
        backup, retention = BackupTag.split(":")
        retention = int(retention)
        
        print "Backup", backup
        print "Retention", retention
        
        if backup == "True":
        
            print "Backup set True for instance %s" % (
                instance['InstanceId'])
        
            try:
                retention_days = retention
            except IndexError:
                retention_days = 7
    
            for dev in instance['BlockDeviceMappings']:
                if dev.get('Ebs', None) is None:
                    continue
                vol_id = dev['Ebs']['VolumeId']
                print "Found EBS volume %s on instance %s" % (
                    vol_id, instance['InstanceId'])
    
                snap = ec.create_snapshot(
                    VolumeId=vol_id,Description="From EBS volume %s on instance %s" % (
                    vol_id, instance['InstanceId']),
                )
    
                to_tag[retention_days].append(snap['SnapshotId'])
    
                print "Retaining snapshot %s of volume %s from instance %s for %d days" % (
                    snap['SnapshotId'],
                    vol_id,
                    instance['InstanceId'],
                    retention_days,
                )


            for retention_days in to_tag.keys():
                delete_date = datetime.date.today() + datetime.timedelta(days=retention_days)
                delete_fmt = delete_date.strftime('%Y-%m-%d')
                print "Will delete %d snapshots on %s" % (len(to_tag[retention_days]), delete_fmt)
                ec.create_tags(
                    Resources=to_tag[retention_days],
                    Tags=[
                        {'Key': 'Expires', 'Value': delete_fmt},
                    ]
                )
        else:
            print "Backup set False for instance %s not performing Backup" % (
                    instance['InstanceId'])