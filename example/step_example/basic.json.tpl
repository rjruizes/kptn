{
  "Comment": "kptn generated state machine for basic",
  "StartAt": "Lane0Parallel",
  "States": {
    "Lane0Parallel": {
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
                  "task_list.$": "$.tasks",
                  "ignore_cache.$": "$.force",
                  "execution_mode": "ecs",
                  "TASKS_CONFIG_PATH": "kptn.yaml",
                  "PIPELINE_NAME": "basic"
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
              "End": true
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
                  "task_list.$": "$.tasks",
                  "ignore_cache.$": "$.force",
                  "execution_mode": "ecs",
                  "TASKS_CONFIG_PATH": "kptn.yaml",
                  "PIPELINE_NAME": "basic"
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
        },
        {
          "StartAt": "list_items_Decide",
          "States": {
            "list_items_Decide": {
              "Type": "Task",
              "Resource": "arn:aws:states:::lambda:invoke",
              "Parameters": {
                "FunctionName": "${decider_lambda_arn}",
                "Payload": {
                  "state.$": "$",
                  "task_name": "list_items",
                  "task_list.$": "$.tasks",
                  "ignore_cache.$": "$.force",
                  "execution_mode": "ecs",
                  "TASKS_CONFIG_PATH": "kptn.yaml",
                  "PIPELINE_NAME": "basic"
                }
              },
              "ResultSelector": {
                "Payload.$": "$.Payload"
              },
              "ResultPath": "$.last_decision",
              "OutputPath": "$",
              "Next": "list_items_Choice"
            },
            "list_items_Choice": {
              "Type": "Choice",
              "Default": "list_items_Skip",
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
                  "Next": "list_items_RunEcs"
                }
              ]
            },
            "list_items_Skip": {
              "Type": "Pass",
              "End": true
            },
            "list_items_RunEcs": {
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
                          "Value": "list_items"
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
                    "Value": "list_items"
                  }
                ]
              },
              "ResultPath": null,
              "End": true
            }
          }
        }
      ],
      "ResultPath": "$.Lane2Parallel",
      "Next": "Lane1Parallel"
    },
    "Lane1Parallel": {
      "Type": "Parallel",
      "Branches": [
        {
          "StartAt": "b_Decide",
          "States": {
            "b_Decide": {
              "Type": "Task",
              "Resource": "arn:aws:states:::lambda:invoke",
              "Parameters": {
                "FunctionName": "${decider_lambda_arn}",
                "Payload": {
                  "state.$": "$",
                  "task_name": "b",
                  "task_list.$": "$.tasks",
                  "ignore_cache.$": "$.force",
                  "execution_mode": "ecs",
                  "TASKS_CONFIG_PATH": "kptn.yaml",
                  "PIPELINE_NAME": "basic"
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
          "StartAt": "process_item_Decide",
          "States": {
            "process_item_Decide": {
              "Type": "Task",
              "Resource": "arn:aws:states:::lambda:invoke",
              "Parameters": {
                "FunctionName": "${decider_lambda_arn}",
                "Payload": {
                  "state.$": "$",
                  "task_name": "process_item",
                  "task_list.$": "$.tasks",
                  "ignore_cache.$": "$.force",
                  "execution_mode": "batch_array",
                  "TASKS_CONFIG_PATH": "kptn.yaml",
                  "PIPELINE_NAME": "basic"
                }
              },
              "ResultSelector": {
                "Payload.$": "$.Payload"
              },
              "ResultPath": "$.last_decision",
              "OutputPath": "$",
              "Next": "process_item_Choice"
            },
            "process_item_Choice": {
              "Type": "Choice",
              "Default": "process_item_Skip",
              "Choices": [
                {
                  "And": [
                    {
                      "Variable": "$.last_decision.Payload.should_run",
                      "BooleanEquals": true
                    },
                    {
                      "Variable": "$.last_decision.Payload.execution_mode",
                      "StringEquals": "batch_array"
                    },
                    {
                      "Variable": "$.last_decision.Payload.array_size",
                      "NumericGreaterThan": 0
                    }
                  ],
                  "Next": "process_item_RunBatch"
                }
              ]
            },
            "process_item_Skip": {
              "Type": "Pass",
              "End": true
            },
            "process_item_RunBatch": {
              "Type": "Task",
              "Resource": "arn:aws:states:::batch:submitJob.sync",
              "Parameters": {
                "JobName.$": "States.Format('basic-process_item-{}', $$.Execution.Name)",
                "JobQueue": "${batch_job_queue_arn}",
                "JobDefinition": "${batch_job_definition_arn}",
                "ArrayProperties": {
                  "Size.$": "$.last_decision.Payload.array_size"
                },
                "ContainerOverrides": {
                  "Environment": [
                    {
                      "Name": "KAPTEN_PIPELINE",
                      "Value": "basic"
                    },
                    {
                      "Name": "KAPTEN_TASK",
                      "Value": "process_item"
                    },
                    {
                      "Name": "DYNAMODB_TABLE_NAME",
                      "Value": "${dynamodb_table_name}"
                    },
                    {
                      "Name": "ARRAY_SIZE",
                      "Value.$": "States.Format('{}', $.last_decision.Payload.array_size)"
                    },
                    {
                      "Name": "KAPTEN_DECISION_REASON",
                      "Value.$": "States.Format('{}', $.last_decision.Payload.reason)"
                    }
                  ]
                },
                "Tags": {
                  "KaptenPipeline": "basic",
                  "KaptenTask": "process_item"
                }
              },
              "ResultPath": null,
              "End": true
            }
          }
        }
      ],
      "ResultPath": "$.Lane2Parallel",
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
          "task_list.$": "$.tasks",
          "ignore_cache.$": "$.force",
          "execution_mode": "ecs",
          "TASKS_CONFIG_PATH": "kptn.yaml",
          "PIPELINE_NAME": "basic"
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
