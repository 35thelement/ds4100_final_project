##############################################
# GBMDB data cleaning and loading into Neo4j #
##############################################

# connect to Neo4j
# make sure to start a database connection in Desktop and verify the port number -- see documentation
con <- neo4j_api$new(
  url = "http://localhost:7474",
  user = "neo4j", 
  password = "neo4j"
)

# clear the database if there's anything in it
call_neo4j("MATCH (n) DETACH DELETE n", con)

##### Read and modify data with R
# read data
BilateralMigration <- select(read.csv("./data/Bilateral_Migration.csv", na.strings=c("","NA")),-4)
Population <- read.csv("./data/Population.csv", na.strings=c("","NA"))
IncomeRegion <- select(read.csv("./data/Country_Metadata.csv", na.strings=c("","NA")), 1, 3, 4)

# rename columns
colnames(BilateralMigration) <- c("Origin_Name","Origin_Code","Migration_by_Gender","Dest_Name","Dest_Code","y1960","y1970","y1980","y1990","y2000")
colnames(Population) <- c("Series_Name","Series_Code","Country_Name","Country_Code","y1960","y1970","y1980","y1990","y2000")
colnames(IncomeRegion) <- c("Country_Code","Income_Name","Region_Name")

# If you run the commented line of code for each of the years, you will see that rows with missing values have no data.
# Therefore we select only rows with complete cases.
#BilateralMigration[!complete.cases(BilateralMigration),] %>% summarise(non_na = sum(!is.na(y2000)))
BilateralMigration <- BilateralMigration[complete.cases(BilateralMigration),]

# gather the years and spread the genders
BilateralMigration <- gather(BilateralMigration, key="Year", value="Mig", y1960, y1970, y1980, y1990, y2000)
BilateralMigration$Year <- BilateralMigration$Year %>% str_sub(2, 5)
BilateralMigration <- spread(BilateralMigration, Migration_by_Gender, Mig)

# Of the rows with missing values in Population, 6 have data for year 1990, and 7 have data for year 2000.
# Therefore we will not filter for only complete cases.
#Population[!complete.cases(Population),] %>% summarise(non_na = sum(!is.na(y2000)))

# Replace "Series_Name" and "Series_Code" in Population with a single field
Population <- Population %>%
  mutate(Population_by_Gender = Series_Code %>%
           str_replace("SP\\.POP\\.TOTL\\.MA\\.IN", "Male") %>%
           str_replace("SP\\.POP\\.TOTL\\.FE\\.IN", "Female") %>%
           str_replace("SP\\.POP\\.TOTL", "Total")) %>%
  select(Country_Name, Country_Code, Population_by_Gender, y1960, y1970, y1980, y1990, y2000)

# Gather the years and spread the genders
Population <- gather(Population, key="Year", value="Pop", y1960, y1970, y1980, y1990, y2000)
Population$Year <- Population$Year %>% str_sub(2, 5)
Population <- Population[complete.cases(Population),] # we can take complete cases now that years are gathered
Population <- spread(Population,Population_by_Gender,Pop)

# Filter Population to only contain data on countries found in BilateralMigration.
# It is ok if there is migration but not population data for some countries.
# Since all countries in BilateralMigration are both origins and destinations (run commented line to verify), we only need use one.
#setdiff(BilateralMigration$Dest_Code, BilateralMigration$Origin_Code)
Population <- Population %>% filter(Country_Code %in% BilateralMigration$Origin_Code)

# IncomeRegion contains rows with missing data.
# All of these are for aggregates and demographic groups, so we'll take only complete cases.
IncomeRegion <- IncomeRegion[complete.cases(IncomeRegion),]

# Create codes for income and region data
IncomeRegion$Income_Code <- IncomeRegion$Income_Name %>% 
  str_replace("High income", "H") %>%
  str_replace("Low income", "L") %>%
  str_replace("Lower middle income", "LM") %>%
  str_replace("Upper middle income", "UM")
IncomeRegion$Region_Code <- IncomeRegion$Region_Name %>%
  str_replace("East Asia & Pacific", "EAP") %>%
  str_replace("Europe & Central Asia", "ECA") %>%
  str_replace("Latin America & Caribbean", "LAC") %>%
  str_replace("Middle East & North Africa", "MENA") %>%
  str_replace("North America", "NA") %>% 
  str_replace("South Asia", "SA") %>%
  str_replace("Sub-Saharan Africa", "SAA")

# If you run the two commented lines below, you'll see that the same 8 rows in IncomeRegion
# do not appear in Population or BilateralMigration. Remove those rows.
#setdiff(IncomeRegion$Country_Code, BilateralMigration$Origin_Code)
#setdiff(IncomeRegion$Country_Code, Population$Country_Code)
IncomeRegion <- IncomeRegion %>% filter(Country_Code %in% Population$Country_Code)

##### Save modified data to csv
write.csv(BilateralMigration, file="./data/final/Bilateral_Migration_Final.csv", row.names=F)
write.csv(BilateralMigration %>% distinct(Origin_Name, Origin_Code), file="./data/final/Country_Final.csv", row.names=F)
write.csv(Population, file="./data/final/Population_Final.csv", row.names=F)
write.csv(IncomeRegion, file="./data/final/Income_Region_Final.csv", row.names=F)

##### Load into Neo4j
# constraints
send_cypher("./constraints.cypher", con)

# years
call_neo4j("CREATE (:Year {year: 1960}), (:Year {year: 1970}), (:Year {year: 1980}), (:Year {year: 1990}), (:Year {year: 2000});", con)

# countries
on_load_country <- 'CREATE (:Country {code: csvLine.Origin_Code, name: csvLine.Origin_Name})'

country_path <- str_c("file://", getwd(), "/data/final/Country_Final.csv")

load_csv(url=country_path, con=con, on_load=on_load_country, as="csvLine", periodic_commit=500)

# migrations
on_load_mig <- 
  'MATCH (o:Country {code: csvLine.Origin_Code})
MATCH (d:Country {code: csvLine.Dest_Code})
MATCH (y:Year {year: toInteger(csvLine.Year)})
CREATE (m:Migration {female: toInteger(csvLine.Female), male: toInteger(csvLine.Male), total: toInteger(csvLine.Total)})
MERGE (o)-[:EMMIGRATION]->(m)-[:IMMIGRATION]->(d)
MERGE (m)-[:IN_YEAR]->(y);'

mig_path <- str_c("file://", getwd(), "/data/final/Bilateral_Migration_Final.csv")

load_csv(url=mig_path, con=con, on_load=on_load_mig, as="csvLine", periodic_commit=500)

# population
on_load_pop <-
  'MATCH (c:Country {code: csvLine.Country_Code})
MATCH (y:Year {year: toInteger(csvLine.Year)})
MERGE (p:Population {total: toInteger(csvLine.Total)})
FOREACH(ignoreMe IN CASE WHEN trim(csvLine.Female) <> "" THEN [1] ELSE [] END | SET p.female = toInteger(csvLine.Female))
FOREACH(ignoreMe IN CASE WHEN trim(csvLine.Male) <> "" THEN [1] ELSE [] END | SET p.male = toInteger(csvLine.Male))
MERGE (c)-[:POPULATION]->(p)-[:IN_YEAR]->(y);'

pop_path <- str_c("file://", getwd(),"/data/final/Population_Final.csv")

load_csv(url=pop_path, con=con, on_load=on_load_pop, as="csvLine", periodic_commit=500)

# income and region
on_load_ir <-
  'MATCH (c:Country {code: csvLine.Country_Code})
MERGE (i:Income {code: csvLine.Income_Code, name: csvLine.Income_Name})
MERGE (r:Region {code: csvLine.Region_Code, name: csvLine.Region_Name})
MERGE (c)-[:INCOME]->(i)
MERGE (c)-[:REGION]->(r);'

ir_path <- str_c("file://", getwd(), "/data/final/Income_Region_Final.csv")

load_csv(url=ir_path, con=con, on_load=on_load_ir, as="csvLine", periodic_commit=500)

# Now that the data is in Neo4j, let's free up some RAM
rm(BilateralMigration, IncomeRegion, Population)
