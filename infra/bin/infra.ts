#!/usr/bin/env node
import * as cdk from "aws-cdk-lib/core";
import { InfraStack } from "../lib/infra-stack";

const app = new cdk.App();
new InfraStack(app, "PolymarketArbBot", {
  env: {
    account: process.env.CDK_DEFAULT_ACCOUNT,
    region: "eu-west-2", // London — closest to Polymarket CLOB
  },
});
