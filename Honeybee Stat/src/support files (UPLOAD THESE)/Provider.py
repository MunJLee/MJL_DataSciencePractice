#DATA CLASS
#A dataframe process classthat returns dataframes in pre-formatted way


import pandas as pd
import numpy as np


class DataProvider:

  #constructor (load the local file - expect this to be packaged in future)  
  def __init__(self):
    try:
      self.raw1 = pd.read_csv('/content/vHoneyNeonic_v03.csv')
      self.raw2 = pd.read_csv('/content/honey.csv')
      self.raw3 = pd.read_csv('/content/stressors.csv')
    
    except FileNotFoundError:
      print("ERROR: File(s) not found")


  #return an unified dataframe
  def getUnifiedDataset(self):
    
    #create a new data frame with column names I want
    newColumns = ['State', 'State Name', 'Region', 'Year', 'Number of Colonies', 'Yield per Colony', 'Total Production', 
                  'Stocks', 'Avrg Price per LB', 'Value of Production', 
                  'Varroa mites', 'Other pests-parasites',	'Diseases',	'Pesticides',	'Stressor-Other',	'Stressor Unknown',
                  'FIPS',	'nCLOTHIANIDIN',	'nIMIDACLOPRID',	'nTHIAMETHOXAM',	'nACETAMIPRID','nTHIACLOPRID', 'nAllNeonic']
    
    unitedDf = pd.DataFrame(columns=newColumns)


    #fill it with the neonic dataset first
    unitedDf['State'] = self.raw1['state']
    unitedDf['State Name'] = self.raw1['StateName']
    unitedDf['Region'] = self.raw1['Region']
    unitedDf['Year'] = self.raw1['year']
    unitedDf['Number of Colonies'] = self.raw1['numcol']
    unitedDf['Yield per Colony'] = self.raw1['yieldpercol']
    unitedDf['Total Production'] = self.raw1['totalprod']
    unitedDf['Stocks'] = self.raw1['stocks']
    unitedDf['Avrg Price per LB'] = self.raw1['priceperlb']
    unitedDf['Value of Production'] = self.raw1['prodvalue']

    unitedDf['FIPS'] = self.raw1['FIPS']
    unitedDf['nCLOTHIANIDIN'] = self.raw1['nCLOTHIANIDIN']
    unitedDf['nIMIDACLOPRID'] = self.raw1['nIMIDACLOPRID']
    unitedDf['nTHIAMETHOXAM'] = self.raw1['nTHIAMETHOXAM']
    unitedDf['nACETAMIPRID'] = self.raw1['nACETAMIPRID']
    unitedDf['nTHIACLOPRID'] = self.raw1['nTHIACLOPRID']
    unitedDf['nAllNeonic'] = self.raw1['nAllNeonic']


    #add missing year's info from the honey dataset
    missingYears = [1989, 1990, 2018, 2019]
    temp1 = self.raw2[self.raw2.Year.isin(missingYears)]

    #change column names for concat
    temp1.columns = ['State', 'Number of Colonies', 'Yield per Colony', 'Total Production',
       'Stocks', 'Avrg Price per LB', 'Value of Production',
       'Year']


    #NOTE: All of following ways work, but can't get the SettingWithCopyWarning turned off,
    #     even using the recommended method.
    #     So I'm just going to go with supposedly the latest way of dealing with this

    #temp1['Number of Colonies'] = temp1['Number of Colonies'] * 1000
    #temp1['Number of Colonies'] = temp1['Number of Colonies'].apply(lambda x: x * 1000)
    #temp1.loc[:, 'Number of Colonies'] *= 1000
    temp1['Number of Colonies'] = temp1['Number of Colonies'].multiply(1000)
    
    temp1['Total Production'] = temp1['Total Production'].multiply(1000)
    temp1['Stocks'] = temp1['Stocks'].multiply(1000)
    temp1['Value of Production'] = temp1['Value of Production'].multiply(1000)
  
    unitedDf = pd.concat([unitedDf, temp1])


    #prepare the stressors dataset
    temp2 = self.raw3.copy()  #create a copy of dataframe
    temp2 = temp2.replace(['(Z)'], 0.01)  #replace usuable data
    temp2 = temp2.replace(['-'], 0.0)  #remove '-' in Other section
    temp2['Year'] = temp2['Year'].apply(lambda x: x[:4]) #removing the quaterly tag

    #turn columns into numeric
    temp2['Varroa mites'] = pd.to_numeric(temp2['Varroa mites'], downcast='signed')
    temp2['Other pests and parasites'] = pd.to_numeric(temp2['Other pests and parasites'], downcast='signed')
    temp2['Diseases'] = pd.to_numeric(temp2['Diseases'], downcast='signed')
    temp2['Pesticides'] = pd.to_numeric(temp2['Pesticides'], downcast='signed')
    temp2['Other'] = pd.to_numeric(temp2['Other'], downcast='signed')
    temp2['Unknown'] = pd.to_numeric(temp2['Unknown'], downcast='signed')

    #flatten the data into average
    temp3 = temp2.groupby(['State', 'Year'])['Varroa mites', 'Other pests and parasites', 
                                             'Diseases', 'Pesticides', 'Other', 'Unknown'].mean().reset_index()

    #conditionally fill the rest of columns with stressors
    for idex, row in temp3.iterrows():
      unitedDf.loc[(unitedDf['State'] == row['State']) & (unitedDf['Year'] == int(row['Year'])), 'Varroa mites'] = row['Varroa mites']
      unitedDf.loc[(unitedDf['State'] == row['State']) & (unitedDf['Year'] == int(row['Year'])), 'Other pests-parasites'] = row['Other pests and parasites']
      unitedDf.loc[(unitedDf['State'] == row['State']) & (unitedDf['Year'] == int(row['Year'])), 'Diseases'] = row['Diseases']
      unitedDf.loc[(unitedDf['State'] == row['State']) & (unitedDf['Year'] == int(row['Year'])), 'Pesticides'] = row['Pesticides']
      unitedDf.loc[(unitedDf['State'] == row['State']) & (unitedDf['Year'] == int(row['Year'])), 'Stressor-Other'] = row['Other']
      unitedDf.loc[(unitedDf['State'] == row['State']) & (unitedDf['Year'] == int(row['Year'])), 'Stressor Unknown'] = row['Unknown']


    #deal with empty values from the honey dataset
    unitedDf['State Name'] = unitedDf['State Name'].fillna(unitedDf['State'].map(unitedDf.dropna().drop_duplicates('State').set_index('State')['State Name']))
    unitedDf['Region'] = unitedDf['Region'].fillna(unitedDf['State'].map(unitedDf.dropna().drop_duplicates('State').set_index('State')['Region']))

    unitedDf.loc[unitedDf['State Name'].isnull(), 'Region'] = 'US'

    unitedDf.loc[unitedDf['State'] == 'CT', 'State Name'] = 'Connecticut'
    unitedDf.loc[unitedDf['State'] == 'DE', 'State Name'] = 'Delaware'
    unitedDf.loc[unitedDf['State'] == 'HI', 'State Name'] = 'Hawaii'
    unitedDf.loc[unitedDf['State'] == 'MD', 'State Name'] = 'Maryland'
    unitedDf.loc[unitedDf['State'] == 'MA', 'State Name'] = 'Messachusetts'
    unitedDf.loc[unitedDf['State'] == 'NV', 'State Name'] = 'Nevada'
    unitedDf.loc[unitedDf['State'] == 'NH', 'State Name'] = 'New Hampshire'
    unitedDf.loc[unitedDf['State'] == 'NM', 'State Name'] = 'New Mexico'
    unitedDf.loc[unitedDf['State'] == 'OK', 'State Name'] = 'Oklahoma'
    unitedDf.loc[unitedDf['State'] == 'RI', 'State Name'] = 'Rhode Island'
    unitedDf.loc[unitedDf['State'] == 'Other', 'State Name'] = 'NA'

    #Clean and sort the data
    unitedDf = unitedDf[unitedDf['State'] != 'US'].sort_values(by=['State', 'Year'], ascending=True)

    return unitedDf


  #return raw
  def getRaw_neonic(self):
    return self.raw1


  def getRaw_honey(self):
    return self.raw2


  def getRaw_stressors(self):
    return self.raw3





