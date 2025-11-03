{
  "Comment": "kptn generated state machine for basic",
  "StartAt": "ParallelRoot",
  "States": {
    "ParallelRoot": {
      "Type": "Parallel",
      "Branches": [
        {
          "StartAt": "a_Decide",
          "States": {
            "a_Decide": {
              "Type": "Task",
              "Resource": "arn:aws:states:::lambda:invoke",
              "Parameters": {
                "FunctionName": "${decider_lambda_arn}",
                "Payload": {
                  "state.$": "$",
                  "task_name": "a",
                  "execution_mode": "ecs"
                }
              },
              "ResultSelector": {
                "Payload.$": "$.Payload"
              },
              "ResultPath": "$.last_decision",
              "OutputPath": "$",
              "Next": "a_Choice"
            },
            "a_Choice": {
              "Type": "Choice",
              "Default": "a_Skip",
              "Choices": [
                {
                  "And": [
                    {
                      "Variable": "$.last_decision.Payload.should_run",
                      "BooleanEquals": true
                    },
                    {
                      "Or": [
                        {
                          "Variable": "$.last_decision.Payload.execution_mode",
                          "StringEquals": "ecs"
                        },
                        {
                          "Not": {
                            "Variable": "$.last_decision.Payload.execution_mode",
                            "IsPresent": true
                          }
                        }
                      ]
                    }
                  ],
                  "Next": "a_RunEcs"
                }
              ]
            },
            "a_Skip": {
              "Type": "Pass",
              "Next": "b_Decide"
            },
            "a_RunEcs": {
              "Type": "Task",
              "Resource": "arn:aws:states:::ecs:runTask.sync",
              "Parameters": {
                "Cluster": "${ecs_cluster_arn}",
                "TaskDefinition": "${ecs_task_definition_arn}",
                "LaunchType": "${launch_type}",
                "NetworkConfiguration": {
                  "AwsvpcConfiguration": {
                    "AssignPublicIp": "${assign_public_ip}",
                    "Subnets": ${subnet_ids},
                    "SecurityGroups": ${security_group_ids}
                  }
                },
                "Overrides": {
                  "ContainerOverrides": [
                    {
                      "Name": "${container_name}",
                      "Environment": [
                        {
                          "Name": "KAPTEN_PIPELINE",
                          "Value": "basic"
                        },
                        {
                          "Name": "KAPTEN_TASK",
                          "Value": "a"
                        },
                        {
                          "Name": "DYNAMODB_TABLE_NAME",
                          "Value": "${dynamodb_table_name}"
                        },
                        {
                          "Name": "KAPTEN_DECISION_REASON",
                          "Value.$": "States.Format('{}', $.last_decision.Payload.reason)"
                        }
                      ]
                    }
                  ]
                },
                "EnableExecuteCommand": true,
                "Tags": [
                  {
                    "Key": "KaptenPipeline",
                    "Value": "basic"
                  },
                  {
                    "Key": "KaptenTask",
                    "Value": "a"
                  }
                ]
              },
              "ResultPath": null,
              "Next": "b_Decide"
            },
            "b_Decide": {
              "Type": "Task",
              "Resource": "arn:aws:states:::lambda:invoke",
              "Parameters": {
                "FunctionName": "${decider_lambda_arn}",
                "Payload": {
                  "state.$": "$",
                  "task_name": "b",
                  "execution_mode": "ecs"
                }
              },
              "ResultSelector": {
                "Payload.$": "$.Payload"
              },
              "ResultPath": "$.last_decision",
              "OutputPath": "$",
              "Next": "b_Choice"
            },
            "b_Choice": {
              "Type": "Choice",
              "Default": "b_Skip",
              "Choices": [
                {
                  "And": [
                    {
                      "Variable": "$.last_decision.Payload.should_run",
                      "BooleanEquals": true
                    },
                    {
                      "Or": [
                        {
                          "Variable": "$.last_decision.Payload.execution_mode",
                          "StringEquals": "ecs"
                        },
                        {
                          "Not": {
                            "Variable": "$.last_decision.Payload.execution_mode",
                            "IsPresent": true
                          }
                        }
                      ]
                    }
                  ],
                  "Next": "b_RunEcs"
                }
              ]
            },
            "b_Skip": {
              "Type": "Pass",
              "End": true
            },
            "b_RunEcs": {
              "Type": "Task",
              "Resource": "arn:aws:states:::ecs:runTask.sync",
              "Parameters": {
                "Cluster": "${ecs_cluster_arn}",
                "TaskDefinition": "${ecs_task_definition_arn}",
                "LaunchType": "${launch_type}",
                "NetworkConfiguration": {
                  "AwsvpcConfiguration": {
                    "AssignPublicIp": "${assign_public_ip}",
                    "Subnets": ${subnet_ids},
                    "SecurityGroups": ${security_group_ids}
                  }
                },
                "Overrides": {
                  "ContainerOverrides": [
                    {
                      "Name": "${container_name}",
                      "Environment": [
                        {
                          "Name": "KAPTEN_PIPELINE",
                          "Value": "basic"
                        },
                        {
                          "Name": "KAPTEN_TASK",
                          "Value": "b"
                        },
                        {
                          "Name": "DYNAMODB_TABLE_NAME",
                          "Value": "${dynamodb_table_name}"
                        },
                        {
                          "Name": "KAPTEN_DECISION_REASON",
                          "Value.$": "States.Format('{}', $.last_decision.Payload.reason)"
                        }
                      ]
                    }
                  ]
                },
                "EnableExecuteCommand": true,
                "Tags": [
                  {
                    "Key": "KaptenPipeline",
                    "Value": "basic"
                  },
                  {
                    "Key": "KaptenTask",
                    "Value": "b"
                  }
                ]
              },
              "ResultPath": null,
              "End": true
            }
          }
        },
        {
          "StartAt": "c_Decide",
          "States": {
            "c_Decide": {
              "Type": "Task",
              "Resource": "arn:aws:states:::lambda:invoke",
              "Parameters": {
                "FunctionName": "${decider_lambda_arn}",
                "Payload": {
                  "state.$": "$",
                  "task_name": "c",
                  "execution_mode": "ecs"
                }
              },
              "ResultSelector": {
                "Payload.$": "$.Payload"
              },
              "ResultPath": "$.last_decision",
              "OutputPath": "$",
              "Next": "c_Choice"
            },
            "c_Choice": {
              "Type": "Choice",
              "Default": "c_Skip",
              "Choices": [
                {
                  "And": [
                    {
                      "Variable": "$.last_decision.Payload.should_run",
                      "BooleanEquals": true
                    },
                    {
                      "Or": [
                        {
                          "Variable": "$.last_decision.Payload.execution_mode",
                          "StringEquals": "ecs"
                        },
                        {
                          "Not": {
                            "Variable": "$.last_decision.Payload.execution_mode",
                            "IsPresent": true
                          }
                        }
                      ]
                    }
                  ],
                  "Next": "c_RunEcs"
                }
              ]
            },
            "c_Skip": {
              "Type": "Pass",
              "End": true
            },
            "c_RunEcs": {
              "Type": "Task",
              "Resource": "arn:aws:states:::ecs:runTask.sync",
              "Parameters": {
                "Cluster": "${ecs_cluster_arn}",
                "TaskDefinition": "${ecs_task_definition_arn}",
                "LaunchType": "${launch_type}",
                "NetworkConfiguration": {
                  "AwsvpcConfiguration": {
                    "AssignPublicIp": "${assign_public_ip}",
                    "Subnets": ${subnet_ids},
                    "SecurityGroups": ${security_group_ids}
                  }
                },
                "Overrides": {
                  "ContainerOverrides": [
                    {
                      "Name": "${container_name}",
                      "Environment": [
                        {
                          "Name": "KAPTEN_PIPELINE",
                          "Value": "basic"
                        },
                        {
                          "Name": "KAPTEN_TASK",
                          "Value": "c"
                        },
                        {
                          "Name": "DYNAMODB_TABLE_NAME",
                          "Value": "${dynamodb_table_name}"
                        },
                        {
                          "Name": "KAPTEN_DECISION_REASON",
                          "Value.$": "States.Format('{}', $.last_decision.Payload.reason)"
                        }
                      ]
                    }
                  ]
                },
                "EnableExecuteCommand": true,
                "Tags": [
                  {
                    "Key": "KaptenPipeline",
                    "Value": "basic"
                  },
                  {
                    "Key": "KaptenTask",
                    "Value": "c"
                  }
                ]
              },
              "ResultPath": null,
              "End": true
            }
          }
        }
      ],
      "Next": "d_Decide"
    },
    "d_Decide": {
      "Type": "Task",
      "Resource": "arn:aws:states:::lambda:invoke",
      "Parameters": {
        "FunctionName": "${decider_lambda_arn}",
        "Payload": {
          "state.$": "$",
          "task_name": "d",
          "execution_mode": "ecs"
        }
      },
      "ResultSelector": {
        "Payload.$": "$.Payload"
      },
      "ResultPath": "$.last_decision",
      "OutputPath": "$",
      "Next": "d_Choice"
    },
    "d_Choice": {
      "Type": "Choice",
      "Default": "d_Skip",
      "Choices": [
        {
          "And": [
            {
              "Variable": "$.last_decision.Payload.should_run",
              "BooleanEquals": true
            },
            {
              "Or": [
                {
                  "Variable": "$.last_decision.Payload.execution_mode",
                  "StringEquals": "ecs"
                },
                {
                  "Not": {
                    "Variable": "$.last_decision.Payload.execution_mode",
                    "IsPresent": true
                  }
                }
              ]
            }
          ],
          "Next": "d_RunEcs"
        }
      ]
    },
    "d_Skip": {
      "Type": "Pass",
      "End": true
    },
    "d_RunEcs": {
      "Type": "Task",
      "Resource": "arn:aws:states:::ecs:runTask.sync",
      "Parameters": {
        "Cluster": "${ecs_cluster_arn}",
        "TaskDefinition": "${ecs_task_definition_arn}",
        "LaunchType": "${launch_type}",
        "NetworkConfiguration": {
          "AwsvpcConfiguration": {
            "AssignPublicIp": "${assign_public_ip}",
            "Subnets": ${subnet_ids},
            "SecurityGroups": ${security_group_ids}
          }
        },
        "Overrides": {
          "ContainerOverrides": [
            {
              "Name": "${container_name}",
              "Environment": [
                {
                  "Name": "KAPTEN_PIPELINE",
                  "Value": "basic"
                },
                {
                  "Name": "KAPTEN_TASK",
                  "Value": "d"
                },
                {
                  "Name": "DYNAMODB_TABLE_NAME",
                  "Value": "${dynamodb_table_name}"
                },
                {
                  "Name": "KAPTEN_DECISION_REASON",
                  "Value.$": "States.Format('{}', $.last_decision.Payload.reason)"
                }
              ]
            }
          ]
        },
        "EnableExecuteCommand": true,
        "Tags": [
          {
            "Key": "KaptenPipeline",
            "Value": "basic"
          },
          {
            "Key": "KaptenTask",
            "Value": "d"
          }
        ]
      },
      "ResultPath": null,
      "End": true
    }
  }
}
