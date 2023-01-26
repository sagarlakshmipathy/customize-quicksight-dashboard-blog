# things to note:
# setup file creation for installing packages: https://stackoverflow.com/a/4527622/8324498

# import statements
import boto3
import json
import time

def update_nested_dict(in_dict, key, value):
    """Replaces the existing value of the key with a new value

    Args:
        in_dict (dict): dictionary to be executed
        key (str): key to search for; example 'DataSetIdentifier' or 'Identifier'...
        value (str): value to replace with; example 'NewValue'

    Returns:
        doesn't return anything but updates the dictionary in place
    """
    for k, v in in_dict.items():
        if key == k:
            in_dict[k] = value
        elif isinstance(v, dict):
            update_nested_dict(v, key, value)
        elif isinstance(v, list):
            for o in v:
                if isinstance(o, dict):
                    update_nested_dict(o, key, value)

# initialize client
qs_client = boto3.client('quicksight')
s3_client = boto3.client('s3')

# initialize the variables
aws_account_id = '733585711144'
region = 'us-east-1'
dashboard_id = 'd280fdec-2adf-45bb-813d-cceece251ec3'
target_dataset_id = 'b40f8cfd-adb1-40c4-b972-b4a0289d7a56'
target_dataset_identifier = 'Strategic'
target_dataset_arn = f'arn:aws:quicksight:{region}:{aws_account_id}:dataset/{target_dataset_id}'
sheet_id = 'd280fdec-2adf-45bb-813d-cceece251ec3_642ba41e-f491-4357-bf4a-a2d4eb9638bc'
visual_id = 'd280fdec-2adf-45bb-813d-cceece251ec3_52d335ed-bbbc-46bd-98d6-000c81a214e6'
replacement_url = 'https://tiny.amazon.com/1dz1oyrsy/aacbs3useaamazawspng'

# user defined target dashboard parameters
target_dashboard_id = 'StrategicDashboard'
target_dashboard_name = 'Strategic'

# describe dashboard definition
dashboard_definition = qs_client.describe_dashboard_definition(AwsAccountId=aws_account_id, DashboardId=dashboard_id)

# Optional: download dashboard definition
json_object = json.dumps(dashboard_definition, indent=4)
with open('./enterpriseDashboard.json', "w") as outfile:
    outfile.write(json_object)

# Optional: upload json object to S3
s3_client.put_object(
    Bucket='aac-bucket-vs', 
    Key=f'definition_files/{dashboard_id}.json',
    Body=json_object
)

# creating object dictionaries
dataset_dict = dashboard_definition['Definition']['DataSetIdentifierDeclarations'][0]
sheets_dict = dashboard_definition['Definition']['Sheets']
for sheet in sheets_dict:
    if sheet['SheetId'] == sheet_id:
        visuals_in_sheet = sheet['Visuals']
        break

# update dataset name and arn in the definition
update_nested_dict(dataset_dict, 'Identifier', target_dataset_identifier)
update_nested_dict(dataset_dict, 'DataSetArn', target_dataset_arn)
update_nested_dict(dashboard_definition, 'DataSetIdentifier', target_dataset_identifier)

# update visual URL in target definition
for visual in visuals_in_sheet:
    try:
       if visual['CustomContentVisual']['VisualId'] == visual_id:
        visual['CustomContentVisual']['ChartConfiguration']['ContentUrl'] = replacement_url

    except KeyError:
        continue

# Optional: download the target definition
json_object = json.dumps(dashboard_definition, indent=4)
with open('./modifiedJson.json', 'w') as outfile:
    json.dumps(json_object)

# Optional: upload json object to S3
s3_client.put_object(
    Bucket='aac-bucket-vs', 
    Key=f'definition_files/{target_dashboard_id}.json',
    Body=json_object
)

# get dashboard permissions
dashboard_permissions = qs_client.describe_dashboard_permissions(AwsAccountId=aws_account_id, DashboardId=dashboard_id)['Permissions']

# create target dashboard
qs_client.create_dashboard(
        AwsAccountId=aws_account_id,
        DashboardId=target_dashboard_id,
        Name=target_dashboard_name,
        Definition=dashboard_definition['Definition'],
        Permissions=dashboard_permissions
    )

# Optional: check dashboard status
time.sleep(2)
print(qs_client.describe_dashboard(AwsAccountId=aws_account_id, DashboardId=target_dashboard_id))
