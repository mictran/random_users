from flatten_json import flatten
import hashlib
import json
import pandas as pd
from urllib.request import urlopen

# Load data from random user API into a JSON data structure
url = "https://randomuser.me/api/?results=20"
response = urlopen(url)
data_json = json.loads(response.read())

# Flat the JSON object
flat_json = flatten(data_json)

# Create the 3 tables random_users_registration, random_users_dimensions, and random_users_pii
# First, identify the number of users that will be loaded into the table
# Then identify the relevant key, value pairs for that user
# For each key in the dictionary, use string matching to line up value to dataframe column
# Finaly, append list to the dataframes

number_Of_Users = flat_json['info_results']

random_Users_Registration_List = []
random_Users_Dimensions_List = []
random_Users_PII_List = []


for i in range(0, number_Of_Users):

    current_random_Users_Registration_List = [None] * 3
    current_random_Users_Dimensions_List = [None] * 13
    current_random_Users_PII_List = [None] * 14

    current_User_Index = "results_" + str(i)

    current_User_Dict = {key: value for key, value in flat_json.items() if key.startswith(current_User_Index)}

    for key in current_User_Dict:

        if key.endswith('login_uuid'):
            current_random_Users_Registration_List[0] = current_User_Dict[key]
            current_random_Users_Dimensions_List[0] = current_User_Dict[key]
            current_random_Users_PII_List[0] = current_User_Dict[key]

        elif key.endswith('registered_date'):
            current_random_Users_Registration_List[1] = current_User_Dict[key][0:10]

        elif key.endswith('registered_age'):
            current_random_Users_Registration_List[2] = current_User_Dict[key]

        elif key.endswith('gender'):
            current_random_Users_Dimensions_List[1] = current_User_Dict[key]

        elif key.endswith('name_title'):
            current_random_Users_Dimensions_List[2] = current_User_Dict[key]

        elif key.endswith('location_city'):
            current_random_Users_Dimensions_List[3] = current_User_Dict[key]

        elif key.endswith('location_state'):
            current_random_Users_Dimensions_List[4] = current_User_Dict[key]

        elif key.endswith('location_country'):
            current_random_Users_Dimensions_List[5] = current_User_Dict[key]

        elif key.endswith('location_postcode'):
            current_random_Users_Dimensions_List[6] = current_User_Dict[key]

        elif key.endswith('location_timezone_offset'):
            current_random_Users_Dimensions_List[7] = current_User_Dict[key]

        elif key.endswith('location_timezone_description'):
            current_random_Users_Dimensions_List[8] = current_User_Dict[key]

        elif key.endswith('dob_age'):
            current_random_Users_Dimensions_List[9] = current_User_Dict[key]

        elif key.endswith('picture_thumbnail'):
            current_random_Users_Dimensions_List[10] = current_User_Dict[key]

        elif key.endswith('nat'):
            current_random_Users_Dimensions_List[11] = current_User_Dict[key]

        elif key.endswith('id_name'):
            current_random_Users_Dimensions_List[12] = current_User_Dict[key]

        elif key.endswith('login_username'):
            current_random_Users_PII_List[1] = current_User_Dict[key]

        elif key.endswith('email'):
            current_random_Users_PII_List[2] = current_User_Dict[key]

        elif key.endswith('name_first'):
            current_random_Users_PII_List[3] = current_User_Dict[key]

        elif key.endswith('name_last'):
            current_random_Users_PII_List[4] = current_User_Dict[key]

        elif key.endswith('location_street_number'):
            current_random_Users_PII_List[5] = current_User_Dict[key]

        elif key.endswith('location_street_name'):
            current_random_Users_PII_List[6] = current_User_Dict[key]

        elif key.endswith('latitude'):
            current_random_Users_PII_List[7] = current_User_Dict[key]

        elif key.endswith('longitude'):
            current_random_Users_PII_List[8] = current_User_Dict[key]

        elif key.endswith('password'):
            if current_User_Dict[key] == '':
                current_random_Users_PII_List[9] = 'Password Unknown'
            else:
                current_random_Users_PII_List[9] = hashlib.md5(current_User_Dict[key].encode()).hexdigest()

        elif key.endswith('dob_date'):
            current_random_Users_PII_List[10] = current_User_Dict[key][0:10]

        elif key.endswith('phone'):
            current_random_Users_PII_List[11] = current_User_Dict[key]

        elif key.endswith('cell'):
            current_random_Users_PII_List[12] = current_User_Dict[key]

        elif key.endswith('id_value'):
            if current_User_Dict[key] is None:
                current_random_Users_PII_List[13] = 'National Identifier Unknown'
            else:
                current_random_Users_PII_List[13] = hashlib.md5(current_User_Dict[key].encode()).hexdigest()

        else:
            continue

    random_Users_Registration_List.append(current_random_Users_Registration_List)
    random_Users_Dimensions_List.append(current_random_Users_Dimensions_List)
    random_Users_PII_List.append(current_random_Users_PII_List)


random_Users_Registration_Table = pd.DataFrame(random_Users_Registration_List, columns = ['user_id', 'registration_date', 'account_age_in_years'])

random_Users_Dimensions_Table = pd.DataFrame(random_Users_Dimensions_List, columns = ['user_id', 'gender', 'title', 'city', 'state', 'country', 'postal_code',
    'timezone_offset', 'timezone_location_description', 'user_age_in_years', 'picture_thumbnail_url', 'nationality', 'national_identifier_system_name'])

random_Users_PII_Table = pd.DataFrame(random_Users_PII_List, columns = ['user_id', 'username', 'email', 'first_name', 'last_name', 'street_number', 'street_name',
 'address_latitude', 'address_longitude', 'user_password_md5', 'date_of_birth', 'landline_phone_number', 'mobile_phone_number', 'national_identifier_md5'])

random_Users_Registration_Table.to_csv(path_or_buf = '/Users/mike/Documents/TakeHome/branch/random_user_registration_table.csv', index = False)
random_Users_Dimensions_Table.to_csv(path_or_buf = '/Users/mike/Documents/TakeHome/branch/random_user_dimensions_table.csv', index = False)
random_Users_PII_Table.to_csv(path_or_buf = '/Users/mike/Documents/TakeHome/branch/random_user_pii_table.csv', index = False)