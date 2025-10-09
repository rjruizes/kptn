{
  "Comment": "Kapten generated state machine for basic2",
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
                  "Value": "basic2"
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
            "Value": "basic2"
          },
          {
            "Key": "KaptenTask",
            "Value": "a"
          }
        ]
      },
      "ResultPath": null,
      "Next": "b"
    },
    "b": {
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
