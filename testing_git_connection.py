import requests, git, yaml, string, random, mysql.connector as sql


# connect to github repo and show available files
def list_files_and_folders(repo_url):
    """Show all files in the repo"""
    api_url = repo_url.replace("github.com", "api.github.com/repos") + "/contents"
    response = requests.get(api_url)
    
    if response.status_code == 200:
        contents = response.json()
        print(f"\nFiles on {repo_url}:\n")
        for item in contents:
            print(item["name"])
        print()
    else:
        print("Failed to retrieve contents. Status code:", response.status_code)

repo_url = "https://github.com/Zla71/weather_repo"
list_files_and_folders(repo_url)


# clone repo or pull repo
def clone_pull_repo():
    cities_from_yml = []
    try:
        local_path = r"D:\Git\weather_repo"
        git.Repo.clone_from(repo_url, local_path)
        print("Repository successfully cloned.\n")
    except:
        repo = git.Repo(local_path)
        origin = repo.remote(name='origin')
        origin.pull()
        print("Repository successfully updated.\n")


    # read yml file from repo        
    with open(r"D:\Git\weather_repo\cities.yml", "r") as ymlfile:
        file = yaml.safe_load(ymlfile)
        # print(file["Europe"])
        print(f"Available European capitals: {file['Europe']}")
        print(f"Available Asian capitals: {file['Asia']}")
        print(f"Available African capitals: {file['Africa']}")
        print(f"Available South American capitals: {file['South_America']}")
        print(f"Available North American capitals: {file['North_America']}")

        # populating the dict to create rows in metadata table
        for continent in file:
            cities = file[continent]
            for city in cities:
                if city not in active_cities_dict:
                    active_cities_dict[city] = [random_string(30), f"bsid_{random_string(30)}", "Active", continent]

        for continent in file:
            cities = file[continent]
            for city in cities:
                cities_from_yml.append(city)    

        return cities_from_yml    

print()

# create 30 symbol random string
def random_string(length):
    symbols = string.ascii_lowercase + string.digits
    result = ''.join(random.choice(symbols) for i in range(length))

    return result

clientid = f"{random_string(30)}"
bsid = f"bsid_{random_string(30)}"

active_cities_dict = {}
inactive_cities_dict = {"Moscow": [random_string(30), f"bsid_{random_string(30)}", "Inactive", "Europe"],
                        "Tallinn": [random_string(30), f"bsid_{random_string(30)}", "Inactive", "Europe"],
                        "Helsinki": [random_string(30), f"bsid_{random_string(30)}", "Inactive", "Europe"],
                        "Pristina": [random_string(30), f"bsid_{random_string(30)}", "Inactive", "Europe"],
                        "Maseru": [random_string(30), f"bsid_{random_string(30)}", "Inactive", "Africa"],
                        "Tripoli": [random_string(30), f"bsid_{random_string(30)}", "Inactive", "Africa"],
                        "Nouakchott": [random_string(30), f"bsid_{random_string(30)}", "Inactive", "Africa"],
                        "Abuja": [random_string(30), f"bsid_{random_string(30)}", "Inactive", "Africa"],
                        "Jakarta": [random_string(30), f"bsid_{random_string(30)}", "Inactive", "Asia"],
                        "Tehran": [random_string(30), f"bsid_{random_string(30)}", "Inactive", "Asia"],
                        "Bishkek": [random_string(30), f"bsid_{random_string(30)}", "Inactive", "Asia"],
                        "Kuala Lumpur": [random_string(30), f"bsid_{random_string(30)}", "Inactive", "Asia"],
                        "Santiago": [random_string(30), f"bsid_{random_string(30)}", "Inactive", "South America"],
                        "Sucre": [random_string(30), f"bsid_{random_string(30)}", "Inactive", "South America"],
                        "Quito": [random_string(30), f"bsid_{random_string(30)}", "Inactive", "South America"],
                        "Paramaribo": [random_string(30), f"bsid_{random_string(30)}", "Inactive", "South America"],
                        }

# clone repo if needed, pull repo data and return all cities from yml file on repo
yml_file_cities = clone_pull_repo()
print()

print(f"All cities from yml file are:\n{yml_file_cities}")

# load cities in metadata table in database
def connect_to_sql_executre_query(cities: dict):

    for city, info in cities.items():
        val = (info[0], info[1], info[2], city, info[3])

        db = sql.connect(
                host = "127.0.0.1",
                port = "3306",
                user = "root",
                database = "wh_metadata",)
        mycoursor = db.cursor()
        sql_query = "INSERT INTO metadata (clientid, bsid, status, cityname, continentname) VALUES (%s,%s,%s,%s,%s)"
        mycoursor.execute(sql_query, val)
        db.commit()
        print(f"{city} is onboarded on database as {info[2]}")
    
# connect_to_sql_executre_query(inactive_cities_dict)


# select all active cities
def active_cities(continent = None):
    db = sql.connect(
            host = "127.0.0.1",
            port = "3306",
            user = "root",
            database = "wh_metadata",)
    mycoursor = db.cursor()
    if not continent:
        sql_query = "select cityname from wh_metadata.metadata where status like 'Act%'"
    else:
        sql_query = f"select cityname from wh_metadata.metadata where status like 'Act%' and continentname = '{continent}'"
    final_result = []
    mycoursor.execute(sql_query)
    result_from_query = mycoursor.fetchall()
    
    for city in result_from_query:
        final_result.append(city[0])

    return final_result


print(f"\nActive cities in database are:\n{active_cities()}")