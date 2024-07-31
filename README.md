import datetime 
import pandas as pd
import pymongo
import warnings
import numpy as np
from transformers import AutoTokenizer, AutoModelForSequenceClassification
import torch
import re
from matplotlib.patches import FancyBboxPatch
import os
os.environ['CURL_CA_BUNDLE'] = ''
warnings.filterwarnings("ignore")
# client = pymongo.MongoClient('10.128.96.25', 27017)
# db = client["news"]

mydf33 = data


newsCollection = mydf33

newsCollection = newsCollection.reset_index(drop=True)
## delete empty rows

i = 0
while i <= len(newsCollection):
    if newsCollection['headline'][i] == []:
        newsCollection = newsCollection.drop(i)
    i += 1
        

        




sectors = [  'Diversified Telecommunication Services', 'Entertainment','Interactive Media & Services','Media',
             'Wireless Telecommunication Services','Show Consumer Discretionary details','Consumer Discretionary','Auto Components',
             'Automobiles','Distributors','Diversified Consumer Services','Hotels, Restaurants & Leisure','Household Durables',
             'Internet & Direct Marketing Retail','Leisure Products','Multiline Retail','Specialty Retail','Textiles, Apparel & Luxury Goods',
             'Consumer Staples','Beverages','Food & Staples Retailing','Food Products','Household Products','Personal Products','Tobacco',
             'Show Energy details','Energy','2 Industries','Energy Equipment & Services','Oil, Gas & Consumable Fuels','Financials','Banks',
             'Capital Markets','Consumer Finance','Diversified Financial Services','Insurance','Mortgage REITs','Thrifts & Mortgage Finance',
             'Show Health Care details','Health Care','6 Industries','Biotechnology','Health Care Equipment & Supplies',
             'Health Care Providers & Services','Health Care Technology','Life Sciences Tools & Services','Pharmaceuticals',
             'Industrials','Aerospace & Defense','Air Freight & Logistics','Airlines','Building Products','Commercial Services & Supplies',
             'Construction & Engineering','Electrical Equipment','Industrial Conglomerates','Machinery','Marine','Professional Services',
             'Road & Rail','Trading Companies & Distributors','Transportation Infrastructure','Information Technology','Communications Equipment',
             'Electronic Equipment, Instruments & Components','IT Services','Semiconductors','Semiconductor Equipment','Software',
             'Technology Hardware, Storage & Peripherals','Materials','Chemicals','Construction Materials','Containers & Packaging',
             'Metals & Mining','Paper & Forest Products','Real Estate','Equity Real Estate Investment Trusts',
             'Real Estate Management & Development','Show Utilities details','Utilities','Electric Utilities',
             'Gas Utilities','Independent Power and Renewable Electricity Producers','Multi-Utilities','Water Utilities']


others = [   "EMERGING MARKET", "GOLD", 
             'Nasdaq', 'S&P 500', "Dow Jones", "Russell 2000", "Brent Crude Oil", "Brent Petrol", "Gold Price" "Ons Gold",
             "FTSE", "Nikkei", "Vix", "Dax", "Dax 40", "Euro Stoxx", "NYSE", "Silver", "Treasury Yields", 
             "COVID", "covid","Trade War","energy shortage", "Energy Prices",
             "stagflasion", "Inflation", "recession"]
usTickers = ['3M', 'A. O. Smith','Abbott Laboratories','AbbVie','Accenture','Activision Blizzard',
             'ADM (company)','Adobe Inc.','ADP (company)','Advance Auto Parts','AES Corporation','Aflac','Agilent Technologies',
             'Air Products and Chemicals','Akamai','Alaska Air Group','Albemarle Corporation','Alexandria Real Estate Equities',
             'Align Technology','Allegion','Alliant Energy','Allstate','Alphabet Inc.','Alphabet Inc.','Altria','Amazon','Amcor',
             'Advanced Micro Devices','Ameren','American Airlines Group','American Electric Power','American Express','American International Group',
             'American Tower','American Water Works','Ameriprise Financial','AmerisourceBergen','Ametek','Amgen','Amphenol',
             'Analog Devices','Ansys','Aon (company)','APA Corporation','Apple Inc.','Applied Materials','Aptiv',
             'Arch Capital Group','Arista Networks','Arthur J. Gallagher & Co.','Assurant','AT&T','Atmos Energy',
             'Autodesk','AutoZone','AvalonBay Communities','Avery Dennison','Baker Hughes','Ball Corporation',
             'Bank of America','Bath & Body Works, Inc.','Baxter International','BD (company)','W. R. Berkley Corporation',
             'Berkshire Hathaway','Best Buy','Bio-Rad','Bio-Techne','Biogen','BlackRock','BNY Mellon','Boeing','Booking Holdings',
             'BorgWarner','Boston Properties','Boston Scientific','Bristol Myers Squibb','Broadcom Inc.','Broadridge Financial Solutions',
             'Brown & Brown','Brown–Forman','C.H. Robinson','Cadence Design Systems','Caesars Entertainment (2020)','Camden Property Trust',
             'Campbell Soup Company','Capital One','Cardinal Health','CarMax','Carnival Corporation & plc','Carrier Global','Catalent',
             'Caterpillar Inc.','Cboe Global Markets','CBRE Group','CDW','Celanese','Centene Corporation','CenterPoint Energy',
             'Ceridian','CF Industries','Charles River Laboratories','Charles Schwab Corporation','Charter Communications',
             'Chevron Corporation','Chipotle Mexican Grill','Chubb Limited','Church & Dwight','Cigna','Cincinnati Financial',
             'Cintas','Cisco','Citigroup','Citizens Financial Group','Clorox','CME Group','CMS Energy','The Coca-Cola Company',
             'Cognizant','Colgate-Palmolive','Comcast','Comerica','Conagra Brands','ConocoPhillips','Consolidated Edison',
             'Constellation Brands','Constellation Energy','CooperCompanies','Copart','Corning Inc.','Corteva','CoStar Group',
             'Costco','Coterra','Crown Castle','CSX Corporation','Cummins','CVS Health','D.R. Horton','Danaher Corporation',
             'Darden Restaurants','DaVita Inc.','John Deere','Delta Air Lines','Dentsply Sirona','Devon Energy','Dexcom',
             'Diamondback Energy','Digital Realty','Discover Financial','Dish Network','Disney','Dollar General',
             'Dollar Tree','Dominion Energy',"Domino's",'Dover Corporation','Dow Inc.','DTE Energy','Duke Energy',
             'DuPont','DXC Technology','Eastman Chemical Company','Eaton Corporation','EBay','Ecolab','Edison International',
             'Edwards Lifesciences','Electronic Arts','Elevance Health','Eli Lilly and Company','Emerson Electric','Enphase',
             'Entergy','EOG Resources','EPAM Systems','EQT','Equifax','Equinix','Equity Residential','Essex Property Trust',
             'The Estée Lauder Companies','Etsy','Everest Re','Evergy','Eversource','Exelon','Expedia Group','Expeditors International',
             'Extra Space Storage','ExxonMobil','F5, Inc.','FactSet','Fastenal','Federal Realty','FedEx','Fifth Third Bank',
             'First Republic Bank','First Solar','FirstEnergy','FIS (company)','Fiserv','Fleetcor','FMC Corporation',
             'Ford Motor Company','Fortinet','Fortive','Fox Corporation','Fox Corporation','Franklin Templeton',
             'Freeport-McMoRan','Garmin','Gartner','Gen Digital','Generac','General Dynamics','General Electric',
             'General Mills','General Motors','Genuine Parts Company','Gilead Sciences','Globe Life','Global Payments',
             'Goldman Sachs','Halliburton','The Hartford','Hasbro','HCA Healthcare','Healthpeak Properties','Henry Schein',
             'The Hershey Company','Hess Corporation','Hewlett Packard Enterprise','Hilton Worldwide','Hologic','The Home Depot',
             'Honeywell','Hormel Foods','Host Hotels & Resorts','Howmet Aerospace','HP Inc.','Humana','Huntington Bancshares',
             'Huntington Ingalls Industries','IBM','IDEX Corporation','Idexx Laboratories','Illinois Tool Works','Illumina, Inc.',
             'Incyte','Ingersoll Rand','Intel','Intercontinental Exchange','International Paper','The Interpublic Group of Companies',
             'International Flavors & Fragrances','Intuit','Intuitive Surgical','Invesco','Invitation Homes','IQVIA','Iron Mountain (company)',
             'J.B. Hunt','Jack Henry & Associates','Jacobs Solutions','Johnson & Johnson','Johnson Controls','JPMorgan Chase','Juniper Networks',
             "Kellogg's",'Keurig Dr Pepper','KeyBank','Keysight','Kimberly-Clark','Kimco Realty','Kinder Morgan','KLA Corporation',
             'Kraft Heinz','Kroger','L3Harris','LabCorp','Lam Research','Lamb Weston','Las Vegas Sands','Leidos','Lennar',
             'Lincoln Financial','Linde plc','Live Nation Entertainment','LKQ Corporation','Lockheed Martin','Loews Corporation',
             "Lowe's",'Lumen Technologies','LyondellBasell','M&T Bank','Marathon Oil','Marathon Petroleum','MarketAxess',
             'Marriott International','Marsh McLennan','Martin Marietta Materials','Masco','Mastercard','Match Group',
             'McCormick & Company',"McDonald's",'McKesson','Medtronic','Merck & Co.','Meta Platforms','MetLife','Mettler Toledo',
             'MGM Resorts','Microchip Technology','Micron Technology','Microsoft','Mid-America Apartment Communities','Moderna',
             'Mohawk Industries','Molina Healthcare','Molson Coors Beverage Company','Mondelez International','Monolithic Power Systems',
             'Monster Beverage',"Moody's Corporation",'Morgan Stanley','The Mosaic Company','Motorola Solutions','MSCI','Nasdaq, Inc.',
             'NetApp','Netflix','Newell Brands','Newmont','News Corp','News Corp','NextEra Energy','Nike, Inc.','NiSource',
             'Nordson Corporation','Norfolk Southern Railway','Northern Trust','Northrop Grumman','Norwegian Cruise Line Holdings',
             'NRG Energy','Nucor','Nvidia','NVR, Inc.','NXP Semiconductors',"O'Reilly Auto Parts",'Occidental Petroleum',
             'Old Dominion Freight Line','Omnicom Group','ON Semiconductor','Oneok','Oracle Corporation','Organon & Co.',
             'Otis Worldwide','Paccar','Packaging Corporation of America','Paramount Global','Parker Hannifin','Paychex',
             'Paycom','PayPal','Pentair','PepsiCo','PerkinElmer','Pfizer','PG&E','Philip Morris International','Phillips 66',
             'Pinnacle West','Pioneer Natural Resources','PNC Financial Services','Pool Corporation (page does not exist)',
             'PPG Industries','PPL Corporation','Principal Financial Group','Procter & Gamble','Progressive Corporation',
             'Prologis','Prudential Financial','Public Service Enterprise Group','PTC (software company)','Public Storage',
             'PulteGroup','Qorvo','Quanta Services','Qualcomm','Quest Diagnostics','Ralph Lauren Corporation','Raymond James',
             'Raytheon Technologies','Realty Income','Regency Centers','Regeneron','Regions Financial Corporation','Republic Services',
             'ResMed','Robert Half','Rockwell Automation','Rollins, Inc.','Roper Technologies','Ross Stores','Royal Caribbean Group',
             'S&P Global','Salesforce','SBA Communications','Schlumberger','Seagate Technology','Sealed Air','Sempra Energy',
             'ServiceNow','Sherwin-Williams','Signature Bank','Simon Property Group','Skyworks Solutions',
             'The J.M. Smucker Company','Snap-on','SolarEdge','Southern Company','Southwest Airlines','Stanley Black & Decker',
             'Starbucks','State Street Corporation','Steel Dynamics','Steris','Stryker Corporation','Silicon Valley Bank',
             'Synchrony Financial','Synopsys','Sysco','T-Mobile US','T. Rowe Price','Take-Two Interactive','Tapestry, Inc.',
             'Targa Resources','Target Corporation','TE Connectivity','Teledyne Technologies','Teleflex','Teradyne','Tesla, Inc.',
             'Texas Instruments','Textron','Thermo Fisher Scientific','TJX Companies','Tractor Supply','Trane Technologies',
             'TransDigm Group','The Travelers Companies','Trimble Inc.','Truist Financial','Tyler Technologies','Tyson Foods',
             'U.S. Bank','UDR, Inc.','Ulta Beauty','Union Pacific Corporation','United Airlines Holdings','United Parcel Service',
             'United Rentals','UnitedHealth Group','Universal Health Services','Valero Energy','Ventas (company)','Verisign',
             'Verisk','Verizon','Vertex Pharmaceuticals','VF Corporation','Viatris','Vici Properties','Visa Inc.',
             'Vornado Realty Trust','Vulcan Materials Company','Wabtec','Walgreens Boots Alliance','Walmart',
             'Warner Bros. Discovery','Waste Management (corporation)','Waters Corporation','WEC Energy Group',
             'Wells Fargo','Welltower','West Pharmaceutical Services','Western Digital','WestRock','Weyerhaeuser',
             'Whirlpool Corporation','Williams Companies','Willis Towers Watson','W. W. Grainger','Wynn Resorts',
             'Xcel Energy','Xylem Inc.','Yum! Brands','Zebra Technologies','Zimmer Biomet','Zions Bancorporation','Zoetis']
usTickers = usTickers + sectors + others

usTickers = [each.upper() for each in usTickers]

torch.set_num_threads(6) 
def calculateSentiment(text):
    inputs = tokenizer([text], padding = True, truncation = True, return_tensors='pt')
    outputs = model(**inputs)
    predictions = torch.nn.functional.softmax(outputs.logits, dim=-1)
    model.config.id2label
    #Tweet #Positive #Negative #Neutral
    positive = predictions[:, 0].tolist()
    negative = predictions[:, 1].tolist()
    neutral = predictions[:, 2].tolist()
    
    table = {
             "Positive":positive,
             "Negative":negative, 
             "Neutral":neutral}
    return positive[0], negative[0], neutral[0]

alphabets= "([A-Za-z])"
prefixes = "(Mr|St|Mrs|Ms|Dr)[.]"
suffixes = "(Inc|Ltd|Jr|Sr|Co)"
starters = "(Mr|Mrs|Ms|Dr|He\s|She\s|It\s|They\s|Their\s|Our\s|We\s|But\s|However\s|That\s|This\s|Wherever)"
acronyms = "([A-Z][.][A-Z][.](?:[A-Z][.])?)"
websites = "[.](com|net|org|io|gov)"
digits = "([0-9])"
def split_into_sentences(text):
    text = " " + text + "  "
    text = text.replace("\n"," ")
    text = re.sub(prefixes,"\\1<prd>",text)
    text = re.sub(websites,"<prd>\\1",text)
    text = re.sub(digits + "[.]" + digits,"\\1<prd>\\2",text)
    if "..." in text: text = text.replace("...","<prd><prd><prd>")
    if "Ph.D" in text: text = text.replace("Ph.D.","Ph<prd>D<prd>")
    text = re.sub("\s" + alphabets + "[.] "," \\1<prd> ",text)
    text = re.sub(acronyms+" "+starters,"\\1<stop> \\2",text)
    text = re.sub(alphabets + "[.]" + alphabets + "[.]" + alphabets + "[.]","\\1<prd>\\2<prd>\\3<prd>",text)
    text = re.sub(alphabets + "[.]" + alphabets + "[.]","\\1<prd>\\2<prd>",text)
    text = re.sub(" "+suffixes+"[.] "+starters," \\1<stop> \\2",text)
    text = re.sub(" "+suffixes+"[.]"," \\1<prd>",text)
    text = re.sub(" " + alphabets + "[.]"," \\1<prd>",text)
    if "”" in text: text = text.replace(".”","”.")
    if "\"" in text: text = text.replace(".\"","\".")
    if "!" in text: text = text.replace("!\"","\"!")
    if "?" in text: text = text.replace("?\"","\"?")
    text = text.replace(".",".<stop>")
    text = text.replace("?","?<stop>")
    text = text.replace("!","!<stop>")
    text = text.replace("<prd>",".")
    sentences = text.split("<stop>")
    sentences = sentences[:-1]
    sentences = [s.strip() for s in sentences]
    sentences = [s.upper() for s in sentences]
    return sentences
proxies = {
    "https" : "fnyproxy.fnylocal.com:8080",
    "http" : "fnyproxy.fnylocal.com:8080"
}


newsCollection.reset_index(inplace = True, drop = True)
#a["date"] = a["date"].apply(lambda x:  x[:10] if type(x) != type(datetime.datetime(2022,11,11)) else x)
#a["date"] = a["date"].apply(lambda x:  datetime.datetime.strptime(x, '%Y-%m-%d') if type(x) != type(datetime.datetime(2022,11,11)) else x)
# a["date"] = a["date"].apply(lambda x: x.replace(hour = 0, minute = 0, second = 0, microsecond = 0))
i = 0
for d in newsCollection["date"]:
    newsCollection["date"][i] = str(d)[:10]
    i += 1
print(newsCollection["date"])

#a = a[a["date"] >= datetime.datetime(2023,8,19)]
# a = a.reset_index().drop("index", axis = 1)
a = newsCollection
a = a.iloc[-105:]
array = np.array(a.headline)
print(len(a))

tokenizer = AutoTokenizer.from_pretrained("ProsusAI/finbert", proxies = proxies)
model = AutoModelForSequenceClassification.from_pretrained("ProsusAI/finbert", proxies = proxies)
def historicalSentiment(df):
    tempTickers = []
    tempPosList = []
    tempNegList = []
    tempNeuList = []
    sentence_count = 0
    ticker_count = 0
    for i in range(len(df)):
        sentenceList = split_into_sentences(df.alltext[i])
        for sentence in sentenceList:
            sentence_count = sentence_count + 1 
            for ticker in usTickers:
                if ticker in sentence:
                    ticker_count = ticker_count +1 
                    tempTickers.append(ticker)
                    sentiment = calculateSentiment(sentence)
                    tempPosList.append(sentiment[0])
                    tempNegList.append(sentiment[1])
                    tempNeuList.append(sentiment[2])
    resultDf = pd.DataFrame(list(zip(tempTickers,tempPosList,tempNegList,tempNeuList)), columns = ["ticker", "positive", "negative", "neutral"])
    resultDf["positive"] = resultDf["positive"]*100
    resultDf["negative"] = resultDf["negative"]*100
    resultDf["neutral"] = resultDf["neutral"]*100
    resultDf["date"] = df.date[i]
    resultDf = resultDf.groupby(["ticker"]).mean()
    return resultDf








df_list = []
for date in a.date.drop_duplicates():
    print(date)
    tempDf = a[a.date == date].copy()
    tempDf.reset_index(inplace = True)
    tempDf.drop("index", axis = 1, inplace = True)
    resultDf = historicalSentiment(tempDf)
    resultDf["date"] = date
    df_list.append(resultDf)
    
ben istiyorum ki aşşağıdaki kodun içeririsinde sentiment analizi yapılsın. Bu analiz row row ilerliyecek. her row içerisinde birden fazla ticker ve bunların sentiment değerleri olabilir bu yüzden bunları hücre içerisinde listede tutlmasını istiyorum. bana kodları düzenliyip yazar mısın? Bütün kodu tek seferde gönder parça parça kod istemiyorum.    

a['tickers'], a['positive'], a['negative'], a['neutral'] = np.nan, np.nan, np.nan, np.nan
    
for index in range(0,len(a)):
    print(a.iloc[index]['tickers'])
