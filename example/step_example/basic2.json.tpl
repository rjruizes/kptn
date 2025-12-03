{
  "Comment": "kptn generated state machine for basic2",
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
          "PIPELINE_NAME": "basic2"
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
                  "Value": "basic2"
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
            "Value": "basic2"
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
          "task_list.$": "$.tasks",
          "ignore_cache.$": "$.force",
          "execution_mode": "ecs",
          "TASKS_CONFIG_PATH": "kptn.yaml",
          "PIPELINE_NAME": "basic2"
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
                  "Value": "basic2"
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
            "Value": "basic2"
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
}
