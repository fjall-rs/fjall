let machines = [
  # Fly.io performance
  # "fly.performance.1x",
  # "fly.performance.2x",
  "fly.performance.4x",
  # "fly.performance.8x",
  # "fly.performance.16x",

  # EC2 T2
  # "aws.ec2.t2.nano",
  # "aws.ec2.t2.micro",
  # "aws.ec2.t2.small",
  # "aws.ec2.t2.medium",
  # "aws.ec2.t2.large",
  # "aws.ec2.t2.xlarge",
  # "aws.ec2.t2.2xlarge",

  # EC2 T3
  # "aws.ec2.t3.nano",
  # "aws.ec2.t3.micro",
  # "aws.ec2.t3.small",
  "aws.ec2.t3.medium",
  # "aws.ec2.t3.large",
  # "aws.ec2.t3.xlarge",
  # "aws.ec2.t3.2xlarge",

  # EC2 T3a
  # "aws.ec2.t3a.nano",
  # "aws.ec2.t3a.micro",
  # "aws.ec2.t3a.small",
  # "aws.ec2.t3a.medium",
  # "aws.ec2.t3a.large",
  # "aws.ec2.t3a.xlarge",
  # "aws.ec2.t3a.2xlarge",

  # EC2 T4g
  # "aws.ec2.t4g.nano",
  # "aws.ec2.t4g.micro",
  # "aws.ec2.t4g.small",
  # "aws.ec2.t4g.medium",
  # "aws.ec2.t4g.large",
  # "aws.ec2.t4g.xlarge",
  # "aws.ec2.t4g.2xlarge",

  # EC2 M4
  # "aws.ec2.m4.large",
]

let table = $env.TABLE_NAME
let commit = $env.COMMIT

print $"Queuing ($commit)"

for machine in $machines {
  let q_pk = $"q#($machine)"
  
  print $"Adding queue item for ($machine)"
  let item = {
    pk: { S: $q_pk },
    sk: { S: $commit },
    version: { S: "2" },
  }
  aws dynamodb put-item --table-name $table --item ($item | to json)
}
