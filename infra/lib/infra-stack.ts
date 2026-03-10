import * as cdk from "aws-cdk-lib/core";
import * as ec2 from "aws-cdk-lib/aws-ec2";
import * as iam from "aws-cdk-lib/aws-iam";
import { Construct } from "constructs";

export class InfraStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    // VPC — default VPC with public subnet only (no NAT gateway cost)
    const vpc = new ec2.Vpc(this, "Vpc", {
      maxAzs: 1,
      natGateways: 0,
      subnetConfiguration: [
        {
          name: "Public",
          subnetType: ec2.SubnetType.PUBLIC,
          cidrMask: 24,
        },
      ],
    });

    // Security group — SSH + outbound only
    const sg = new ec2.SecurityGroup(this, "BotSg", {
      vpc,
      description: "Polymarket arb bot",
      allowAllOutbound: true,
    });
    sg.addIngressRule(ec2.Peer.anyIpv4(), ec2.Port.tcp(22), "SSH");

    // IAM role (minimal — just SSM for session manager as backup)
    const role = new iam.Role(this, "BotRole", {
      assumedBy: new iam.ServicePrincipal("ec2.amazonaws.com"),
      managedPolicies: [
        iam.ManagedPolicy.fromAwsManagedPolicyName(
          "AmazonSSMManagedInstanceCore"
        ),
      ],
    });

    // Key pair — import existing or create
    const keyPairName = this.node.tryGetContext("keyPairName") || "polymarket-bot";
    const keyPair = new ec2.KeyPair(this, "KeyPair", {
      keyPairName,
      type: ec2.KeyPairType.ED25519,
    });

    // User data — install Node.js, pm2, git
    const userData = ec2.UserData.forLinux();
    userData.addCommands(
      "#!/bin/bash",
      "set -euo pipefail",
      "",
      "# System updates",
      "dnf update -y",
      "dnf install -y git gcc-c++ make",
      "",
      "# Install Node.js 22 via fnm",
      'export HOME=/home/ec2-user',
      'su - ec2-user -c "curl -fsSL https://fnm.vercel.app/install | bash"',
      'su - ec2-user -c "source ~/.bashrc && fnm install 22 && fnm default 22"',
      "",
      "# Install pm2 globally",
      'su - ec2-user -c "source ~/.bashrc && npm install -g pm2"',
      "",
      "# Create app directory",
      "mkdir -p /home/ec2-user/app",
      "chown ec2-user:ec2-user /home/ec2-user/app",
    );

    // EC2 instance — c8g.medium (Graviton4, ARM64)
    const instance = new ec2.Instance(this, "Bot", {
      vpc,
      vpcSubnets: { subnetType: ec2.SubnetType.PUBLIC },
      instanceType: new ec2.InstanceType("c8g.medium"),
      machineImage: ec2.MachineImage.latestAmazonLinux2023({
        cpuType: ec2.AmazonLinuxCpuType.ARM_64,
      }),
      securityGroup: sg,
      role,
      keyPair,
      userData,
      blockDevices: [
        {
          deviceName: "/dev/xvda",
          volume: ec2.BlockDeviceVolume.ebs(20, {
            volumeType: ec2.EbsDeviceVolumeType.GP3,
          }),
        },
      ],
    });

    // Elastic IP so the address doesn't change on restart
    const eip = new ec2.CfnEIP(this, "BotEip");
    new ec2.CfnEIPAssociation(this, "BotEipAssoc", {
      eip: eip.ref,
      instanceId: instance.instanceId,
    });

    // Outputs
    new cdk.CfnOutput(this, "InstanceId", {
      value: instance.instanceId,
    });
    new cdk.CfnOutput(this, "PublicIp", {
      value: eip.attrPublicIp,
    });
    new cdk.CfnOutput(this, "SshCommand", {
      value: `ssh -i ~/.ssh/${keyPairName}.pem ec2-user@\${PublicIp}`,
      description: "SSH command (replace PublicIp)",
    });
    new cdk.CfnOutput(this, "KeyPairId", {
      value: keyPair.keyPairId,
      description: "Download private key from SSM Parameter Store",
    });
  }
}
