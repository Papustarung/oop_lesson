import csv, os

__location__ = os.path.realpath(
    os.path.join(os.getcwd(), os.path.dirname(__file__)))

cities = []
with open(os.path.join(__location__, 'Cities.csv')) as f:
    rows = csv.DictReader(f)
    for r in rows:
        cities.append(dict(r))

countries = []
with open(os.path.join(__location__, 'Countries.csv')) as f:
    rows = csv.DictReader(f)
    for r in rows:
        countries.append(dict(r))

###############################################################

class TableDB:
    def __init__(self):
        self.table_database = []

    def insert(self, table):
        self.table_database.append(table)

    def search(self, table_name):
        for table in self.table_database:
            if table.table_name == table_name:
                return table

class Table:
    def __init__(self, table_name, table):
        self.table_name = table_name
        self.table = table

    def filter(self, condition):
        filtered_list = []
        for item in self.table:
            if condition(item):
                filtered_list.append(item)
        return filtered_list

    def aggregate(self, aggregation_function, aggregation_key, condition=None):
        value = []
        filtered_table = self.table if condition is None else [row for row in self.table if condition(row)]
        for item in filtered_table:
            value.append(float(item[aggregation_key]))
        if not value:
            return None
        return aggregation_function(value)

    def __str__(self):
        pass

table_database = TableDB()
cities_table = Table('cities', cities)
countries_table = Table('countries', countries)
table_database.insert(cities_table)
table_database.insert(countries_table)


for country in countries_table.table:
    max_lat = table_database.search("cities").aggregate(
        lambda x: max(x),
        "latitude",
        lambda x: x['country'] == f"{country["country"]}"
    )
    if max_lat is not None:
        print(f"Maximum latitude of {country['country']} is {max_lat}")
    else:
        print(f"No data available for {country['country']}")

    min_lat = table_database.search("cities").aggregate(
        lambda x: min(x),
        "latitude",
        lambda x: x['country'] == f"{country["country"]}"
    )
    if min_lat is not None:
        print(f"Minimum latitude of {country['country']} is {min_lat}")
    else:
        print(f"No data available for {country['country']}")
    print("-------------------------------------------")
