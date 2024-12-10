import pandas as pd
import requests, json, sys
from io import BytesIO
from azure.identity import DefaultAzureCredential
from msal import ConfidentialClientApplication


class MICROSOFT_GRAPH:
    """
    This class enables authentication and interaction with the Microsoft Graph API
    for SharePoint Online (SPO) operations. It supports two authentication methods
    to obtain an access token: Managed Identity Authentication and Client Secret Authentication.
    """

    def __init__(
        self, client_id: str, client_credential: str, auth_type: str = "secret"
    ) -> None:
        """
        Initializes a new instance of the class.

        Args:
            client_id (str): The client ID for the application registered in Azure Portal.
            client_credential (str): The client secret for the application.
            auth_type (str): The type of authentication to use ('managed' for Managed Identity, 'secret' for Client Secret).
        """
        self.client_id = client_id
        self.client_credential = client_credential
        self.auth_type = auth_type
        self.authority = "https://login.microsoftonline.com/<tenant_id>"  # Insert your Org's Tenant ID
        self.scopes = ["https://graph.microsoft.com/.default"]
        self.access_token = self.__get_access_token()

    def __get_access_token(self) -> str:
        """
        Method to obtain an access token from Microsoft Identity Platform.
        Depending on the authentication type, this method will either use a managed identity
        or a client secret to authenticate.

        Returns:
            str: An access token that can be used to authenticate API requests.
        """
        if self.auth_type == "managed":
            credential = DefaultAzureCredential()
            token_response = credential.get_token(self.scopes)
            return token_response.token
        elif self.auth_type == "secret":
            client = ConfidentialClientApplication(
                client_id=self.client_id,
                authority=self.authority,
                client_credential=self.client_credential,
            )
            token_response = client.acquire_token_for_client(scopes=self.scopes)
            return token_response["access_token"]
        else:
            raise ValueError("Invalid authentication type")

    def _get_folder_id(self, teams_group_id: str, folder_path: str) -> str:
        """
        Retrieves the unique identifier of a folder within a SharePoint site.

        Args:
            teams_group_id (str): The unique identifier for the Teams group.
            folder_path (str): The name of the folder to search for.

        Returns:
            str: The unique identifier of the folder if found, otherwise an empty string.
        """
        folder_url = f"https://graph.microsoft.com/v1.0/groups/{teams_group_id}/drive/root:/{folder_path}"
        headers = {"Authorization": f"Bearer {self.access_token}"}

        try:
            response = requests.get(folder_url, headers=headers)
            response.raise_for_status()
            data = json.loads(response.text)
            folder_id = data["id"]
            return folder_id
        except requests.exceptions.HTTPError as http_err:
            return f"HTTP error occurred: {http_err}"
        except Exception as err:
            return f"Other error occurred: {err}"

    def _get_drive_id(self, site_id: str) -> str:
        """
        Retrieves the drive ID of the 'Documents' folder for a given SharePoint site.

        Args:
            site_id (str): The unique identifier for the SharePoint site.

        Returns:
            str: The drive ID of the 'Documents' folder.
        """
        drives_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives"
        headers = {"Authorization": f"Bearer {self.access_token}"}

        try:
            response = requests.get(drives_url, headers=headers)
            response.raise_for_status()
            drives = response.json().get("value", [])
            for drive in drives:
                if drive["name"] == "Documents":
                    return drive["id"]
        except requests.exceptions.HTTPError as http_err:
            return f"HTTP error occurred: {http_err}"
        except Exception as err:
            return f"Other error occurred: {err}"
        return ""

    def read_sharepoint_excel(
        self,
        teams_group_id: str,
        file_path: str,
        dtype=None,
        sheet_name=0,
        usecols=None,
    ) -> Union[pd.DataFrame, str]:
        """
        Reads an Excel file from SharePoint Online from a specified directory
        and returns as pandas DataFrame.

        Args:
            teams_group_id (str): The unique identifier for the teams group
            file_path (str): The path to the Excel file in SharePoint Online.
            sheet_name (int, str, list, or None, optional): Name or index of the sheet, None to read all sheets. Default to 0.
        Returns:
            A Pandas DataFrame
        """
        file_url = f"https://graph.microsoft.com/v1.0/groups/{teams_group_id}/drive/root:/{file_path}:/content"
        headers = {"Authorization": f"Bearer {self.access_token}"}
        response = requests.get(file_url, headers=headers)
        try:
            response.raise_for_status()
            excel_content = BytesIO(response.excel_content)
            if dtype is not None:
                df = pd.read_excel(
                    excel_content, dtype=dtype, sheet_name=sheet_name, usecols=usecols
                )
            else:
                df = pd.read_excel(
                    excel_content, sheet_name=sheet_name, usecols=usecols
                )
                return df
        except requests.exceptions.HTTPError as http_err:
            return f"HTTP error accured: {http_err}"
        except Exception as err:
            return f"Other error occured: {err}"

    def filter_and_merge_csv_files(
        self, site_id: str, teams_group_id: str, folder_path: str, file_prefix: str
    ):
        """
        Filters and merges CSV files from a Microsoft Teams folder using Microsoft Graph API.

        Parameters:
        - site_id (str): The unique identifier for the SharePoint site.
        - teams_group_id (str): The identifier for the Microsoft Teams group.
        - folder_path (str): The relative path to the folder within the Teams document library.
        - file_prefix (str): The prefix used to filter specific CSV files.

        Returns:
        - pd.DataFrame: A Pandas DataFrame containing the merged content of the filtered CSV files.

        Raises:
        - HTTPError: If any API request fails.
        - ValueError: If no CSV files matching the criteria are found.
        """
        # Fetch folder and drive IDs
        folder_id = self._get_folder_id(teams_group_id, folder_path)
        drive_id = self._get_drive_id(site_id)

        # Build the URL for folder contents
        folder_contents_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive_id}/items/{folder_id}/children"
        # Set up headers for authentication
        headers = {"Authorization": f"Bearer {self.access_token}"}

        # Request folder contents from Microsoft Graph API
        response = requests.get(folder_contents_url, headers=headers)
        response.raise_for_status()

        # Parse the folder contents
        folder_contents = response.json()

        # Initialize a list to store DataFrames
        df_list = []
        print("File merge is in progress...")

        # Check if folder contains any items
        if "value" in folder_contents:
            for item in folder_contents["value"]:
                if "file" in item:  # Ensure the item is a file
                    # Check for files matching the prefix and CSV extension
                    if item["name"].startswith(file_prefix) and item["name"].endswith(
                        ".csv"
                    ):
                        file_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive_id}/items/{item['id']}/content"
                        print(f"Merging file: {item['name']}")

                        # Download the CSV file content
                        file_response = requests.get(file_url, headers=headers)
                        file_response.raise_for_status()

                        # Load the CSV content into a Pandas DataFrame
                        csv_content = BytesIO(file_response.content)
                        df = pd.read_csv(csv_content)
                        df_list.append(df)

            # Check if any DataFrames were created
            if df_list:
                # Combine all DataFrames into one
                combined_df = pd.concat(df_list, ignore_index=True)
                print("File merge completed successfully!")
                return combined_df
            else:
                raise ValueError(
                    "No CSV files matching the specified prefix were found."
                )
        else:
            raise ValueError("The folder is empty or inaccessible.")


def upload_file_to_existing_folder(
    self, teams_group_id: str, folder_path: str, file_name: str
) -> None:
    """
    Uploads a file to an existing folder in a Microsoft Teams group's document library using Microsoft Graph API.

    Parameters:
    - teams_group_id (str): The identifier of the Microsoft Teams group.
    - folder_path (str): The relative path to the folder where the file should be uploaded.
    - file_name (str): The name of the file to upload (including its extension).

    Returns:
    - None

    Raises:
    - HTTPError: If the API request fails.
    - FileNotFoundError: If the specified file is not found locally.
    - ValueError: If there is an issue with the folder path or upload endpoint.
    """
    # Get the folder ID
    folder_id = self._get_folder_id(
        teams_group_id, folder_path
    )  # Ensure correct method call
    upload_endpoint = f"https://graph.microsoft.com/v1.0/groups/{teams_group_id}/drive/items/{folder_id}:/{file_name}:/content"
    headers = {"Authorization": f"Bearer {self.access_token}"}

    try:
        # Read the file content in binary mode
        with open(file_name, "rb") as content_file:
            file_content = content_file.read()

        # Send PUT request to upload the file
        response = requests.put(upload_endpoint, headers=headers, data=file_content)
        response.raise_for_status()  # Raise an error for unsuccessful requests
        print(f"{file_name} has been uploaded successfully!")

    except FileNotFoundError:
        print(f"Error: The file '{file_name}' was not found.")
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
    except Exception as err:
        print(f"An error occurred: {err}")


# implementation
# Import Credentials
sys.path.append(r"C:\Users\.....")
from config import client_id, client_cred, group_id, site_id

# Define file path or folder directory as necessary
file_path = "General/Documents/GroupAPI/temp_data.csv"
folder_dir = "General/Documents/GroupAPI"

sp_graph_obj = MICROSOFT_GRAPH(
    client_id=client_id, client_credential=client_cred, auth_type="secret"
)

df_csv = sp_graph_obj.read_sharepoint_excel(group_id, file_path)
print(df_csv.head())
