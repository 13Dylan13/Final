# -*- coding: utf-8 -*-
"""
Created on Thu Feb  1 08:23:05 2024

@author: DR2806
"""

def Test_Output_Filename():
    #ARRANGE
    from app import get_OutcomeFile_name
    import datetime
    date = datetime.datetime.today()
    filetype = '.csv'
    zipfiletype = '.csv.zip'
    #ACT
    filename = get_OutcomeFile_name(date,filetype)
    zipfilename = get_OutcomeFile_name(date,zipfiletype)
    #ASSERT
    assert filename[0:25] == 'CapitaTFL-CampaignOutcome'
    assert filename[-4:] == '.csv'
    assert zipfilename[-4:] == '.zip'