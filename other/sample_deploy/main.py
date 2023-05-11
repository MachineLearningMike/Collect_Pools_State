
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

def main(req1, req2):
    client = bigquery.Client()
    table_id = "nice-proposal-338510.stg_all_info.tbl_config"
    # TODO(developer): Set table_id to the ID of the table to determine existence.
    # table_id = "your-project.your_dataset.your_table"

    try:
        client.get_table(table_id)  # Make an API request.
        print("Table {} already exists.".format(table_id))
    except NotFound:
        print("Table {} is not found.".format(table_id))