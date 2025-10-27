{
  "Comment": "Kapten generated state machine for basic",
  "StartAt": "ParallelRoot",
  "States": {
    "ParallelRoot": {
      "Type": "Parallel",
      "Branches": [
        {
          "StartAt": "a",
          "States": {
            "a": {
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
              "Next": "PrepareBItems"
            },
            "PrepareBItems": {
              "Type": "Pass",
              "Parameters": {
                "jobQueue": "${batch_job_queue_arn}",
                "jobDefinition": "${batch_job_definition_arn}",
                "arraySize": 3
              },
              "ResultPath": "$.b_batch",
              "Next": "b"
            },
            "b": {
              "Type": "Task",
              "Resource": "arn:aws:states:::batch:submitJob.sync",
              "Parameters": {
                "JobName.$": "States.Format('${pipeline_name}-b-array-{}', $$.Execution.Name)",
                "JobQueue.$": "$.b_batch.jobQueue",
                "JobDefinition.$": "$.b_batch.jobDefinition",
                "ArrayProperties": {
                  "Size.$": "$.b_batch.arraySize"
                },
                "ContainerOverrides": {
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
                      "Name": "ARRAY_SIZE",
                      "Value.$": "States.Format('{}', $.b_batch.arraySize)"
                    }
                  ]
                },
                "Tags": {
                  "KaptenPipeline": "basic",
                  "KaptenTask": "b"
                }
              },
              "ResultPath": null,
              "End": true
            }
          }
        },
        {
          "StartAt": "c",
          "States": {
            "c": {
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
      "Next": "d"
    },
    "d": {
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
