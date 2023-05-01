# things to note:
# setup file creation for installing packages: https://stackoverflow.com/a/4527622/8324498

# import statements
import boto3
import pandas as pd
import json
import io

# initialize client
qs_client = boto3.client('quicksight')
s3_client = boto3.client('s3')

# user supplied variables
aws_account_id = '733585711144'
region = 'us-east-1'
dashboard_id = '9b55e360-306e-4d27-8bb2-22e46c931701'
sheet_id = '9b55e360-306e-4d27-8bb2-22e46c931701_fc141d89-cb07-4619-a7a5-50d9ebd5d85b'
visual_id = '9b55e360-306e-4d27-8bb2-22e46c931701_200b48d4-7e56-4745-ad2f-ee976978f3d6'
bucket_name = 'customize-dashboard-blogwork'
s3_file_path = 'parameters/customization_parameters.csv'

def update_nested_dict(in_dict, key, value, match_value=None):
    """Replaces the existing value of the key with a new value

    Args:
        in_dict (dict): dictionary to be executed
        key (str): key to search for; example 'DataSetIdentifier' or 'Identifier'...
        value (str): value to replace with; example 'NewValue'

    Returns:
        doesn't return anything but updates the dictionary in place
    """
    for k, v in in_dict.items():
        if key == k and v == match_value:
            in_dict[k] = value
        elif isinstance(v, dict):
            update_nested_dict(v, key, value, match_value)
        elif isinstance(v, list):
            for o in v:
                if isinstance(o, dict):
                    update_nested_dict(o, key, value, match_value)

# extracted values
analysis_sheet_id = sheet_id.split('_')[-1]
analysis_visual_id = visual_id.split('_')[-1]

# reading the csv file
obj = s3_client.get_object(Bucket=bucket_name, Key=s3_file_path)
body = obj['Body'].read()
df = pd.read_csv(io.BytesIO(body))

for index, deployment in enumerate(df.iterrows()):
    customer_name = df['customerName'].tolist()[index]
    replacement_url = df['logoUrl'].tolist()[index]
    number_of_datasets = df['no_of_datasets'].tolist()[index]
    source_dataset_id_list = df['source_dataset_ids'].tolist()[index].split(';')
    target_dataset_id_list = df['target_dataset_ids'].tolist()[index].split(';')
    target_analysis_id = target_dashboard_id = df['target_analysis_and_dashboard_id'].tolist()[index]
    target_analysis_name = target_dashboard_name = df['target_analysis_and_dashboard_name'].tolist()[index]

    # derived values
    if target_dataset_id_list:
        source_dataset_identifier_list  = []
        for dataset_id in source_dataset_id_list:
            source_dataset_identifier_list.append(qs_client.describe_data_set(AwsAccountId=aws_account_id, DataSetId=dataset_id)['DataSet']['Name'])

        target_dataset_identifier_list = []
        for dataset_id in target_dataset_id_list:
            target_dataset_identifier_list.append(qs_client.describe_data_set(AwsAccountId=aws_account_id, DataSetId=dataset_id)['DataSet']['Name'])

        source_dataset_arn_list = []
        for dataset_id in source_dataset_id_list:
            source_dataset_arn_list.append(f'arn:aws:quicksight:{region}:{aws_account_id}:dataset/{dataset_id}')

        target_dataset_arn_list = []
        for dataset_id in target_dataset_id_list:
            target_dataset_arn_list.append(f'arn:aws:quicksight:{region}:{aws_account_id}:dataset/{dataset_id}')

    #describe analysis definition
    source_analysis_arn = qs_client.describe_dashboard(
        AwsAccountId=aws_account_id, 
        DashboardId=dashboard_id
    )['Dashboard']['Version']['SourceEntityArn']
    source_analysis_id = source_analysis_arn.split('/')[-1]
    analysis_definition = qs_client.describe_analysis_definition(
        AwsAccountId=aws_account_id, 
        AnalysisId=source_analysis_id
    )

    # creating object dictionaries
    (analysis_definition['Definition']['DataSetIdentifierDeclarations']).sort(key=lambda k: k['Identifier'])
    dataset_dict = analysis_definition['Definition']['DataSetIdentifierDeclarations']
    sheets_dict = analysis_definition['Definition']['Sheets']
    for sheet in sheets_dict:
        if sheet['SheetId'] == analysis_sheet_id:
            visuals_in_sheet = sheet['Visuals']
            break

    # Optional: download dashboard definition
    # json_object = json.dumps(analysis_definition, indent=4)
    # with open('./source_analysis_definition.json', "w") as outfile:
    #     outfile.write(json_object)

    # Optional: upload json object to S3
    # s3_client.put_object(
    #     Bucket='aac-bucket-vs', 
    #     Key=f'definition_files/{dashboard_id}.json',
    #     Body=json_object
    # )

    # update dataset identifier and arn in the dataset definition
    if target_dataset_id_list:
        try:
            for number, (source_dataset_identifier, target_dataset_identifier) in enumerate(zip(source_dataset_identifier_list, target_dataset_identifier_list)):
                update_nested_dict(dataset_dict[number], 'Identifier', target_dataset_identifier, source_dataset_identifier)
                update_nested_dict(dataset_dict[number], 'DataSetArn', target_dataset_arn_list[number], source_dataset_arn_list[number])
        except IndexError:
            pass

    # update dataset identifier for the rest of the dashboard definition
    if target_dataset_id_list:
        for source_dataset_identifier, target_dataset_identifier in zip(source_dataset_identifier_list, target_dataset_identifier_list):
            update_nested_dict(analysis_definition, 'DataSetIdentifier', target_dataset_identifier, source_dataset_identifier)

    # update visual URL in target definition
    for visual in visuals_in_sheet:
        try:
           if visual['CustomContentVisual']['VisualId'] == analysis_visual_id:
            visual['CustomContentVisual']['ChartConfiguration']['ContentUrl'] = replacement_url

        except KeyError:
            continue

    # Optional: download the target definition
    # json_object = json.dumps(analysis_definition, indent=4, default=str)
    # with open(f'./target_analysis_definition-{index+1}.json', 'w') as outfile:
    #     outfile.write(json_object)

    # Optional: upload json object to S3
    # s3_client.put_object(
    #     Bucket='aac-bucket-vs', 
    #     Key=f'definition_files/{target_dashboard_id}.json',
    #     Body=json_object
    # )

    # delete target dashboard if it exists
    try:
        qs_client.delete_dashboard(AwsAccountId=aws_account_id, DashboardId=target_dashboard_id)
        qs_client.delete_analysis(AwsAccountId=aws_account_id, AnalysisId=target_analysis_id)
    except:
        pass

    # get permissions
    analysis_permissions = qs_client.describe_analysis_permissions(
        AwsAccountId=aws_account_id,
        AnalysisId=source_analysis_id
    )['Permissions']
    dashboard_permissions = qs_client.describe_dashboard_permissions(
        AwsAccountId=aws_account_id, 
        DashboardId=dashboard_id
    )['Permissions']

    # create target dashboard
    qs_client.create_analysis(
        AwsAccountId=aws_account_id,
        AnalysisId=target_analysis_id,
        Name=target_analysis_name,
        Definition=analysis_definition['Definition'],
        Permissions=analysis_permissions
    )
    qs_client.create_dashboard(
            AwsAccountId=aws_account_id,
            DashboardId=target_dashboard_id,
            Name=target_dashboard_name,
            Definition=analysis_definition['Definition'],
            Permissions=dashboard_permissions
        )

# Optional: check dashboard status
# time.sleep(2)
# print(qs_client.describe_dashboard(AwsAccountId=aws_account_id, DashboardId=target_dashboard_id))
