import requests
from falkordb import FalkorDB

# Call an HTTP GET request to the OData service
def main():

    """
    This function calls an OData service and prints the response.
    """

    # Connect to FalkorDB
    db = FalkorDB(host='localhost', port=6379)
    graph = db.select_graph('priority')

    # Call the OData service pass user name and password
    response = requests.get('https://demoen.softsolutions.co.il/odata/Priority/tabula.ini/demo/$metadata', auth=('api', '12345'))

    # Print the response
    print(response.text)


if __name__ == "__main__":
    main()
