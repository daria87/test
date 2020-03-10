"""
Author: Daria Alexander
"""

import pandas as pd
import json
import numpy as nm

class eventLogLoader():

    #constructor
    def __init__(self):
        self.events = None # Pandas dataframe

    def load(self, path_json):
        try :
            with open(path_json, encoding='utf-8') as data_file:
                data = json.loads(data_file.read())
                self.events = pd.DataFrame(data)
        except (FileNotFoundError, IOError):
            print ("File not found")
        #rename the columns in order that their names match imputlog format


        #self.events = self.events[self.events.event_type != 1] # delete the events that have eventsType 1
        self.events = self.events[(self.events['exclude'] == False)]

        # поправим тип у дат со строк на datetime64
        self.events['event_time'] = pd.to_datetime(self.events['event_time'])

    def drop(self):
        self.events = self.events[self.events["msg_id"] != 57456]
        self.events = self.events[self.events["msg_id"] != 12337]
        self.events = self.events[self.events["msg_id"] != 57440]
        self.events = self.events[self.events["msg_id"] != 11705]
        self.events = self.events[self.events["msg_id"] != 18056]
        self.events = self.events[self.events["msg_id"] != 57413]
        self.events = self.events[self.events["msg_id"] != 57599]
        self.events = self.events[self.events["msg_id"] != 63356]




    # extract the length difference of the line
    def extractLengthDiff(self):
        dfr = self.events[self.events.event_type_t == 'Text change']
        self.events['characters_delta'] = dfr.event_text.str.len() - dfr.shift(1).event_text.str.len()
        self.events.loc[self.events.event_type_t == 'TTS voicing start', 'characters_delta'] = 1


    def extractPauses(self, minimalPauseTreshold = 250):
        """
        :param minimalPauseTreshold: 200,500, 1000, 2000
        """
        self.events = self.events.assign(timeDiff = self.events.event_time.diff())
        self.events['minimal_pause'] = self.events.timeDiff > pd.Timedelta(str(minimalPauseTreshold)+'ms')
        #self.events['Pause500'] = self.events.timeDiff > pd.Timedelta('500ms')
        #self.events['Pause1000'] = self.events.timeDiff > pd.Timedelta('1000ms')
        #self.events['Pause2000'] = self.events.timeDiff > pd.Timedelta('2000ms')


    def revisions(self):
            self.events['revisions'] = self.events.characters_delta <= -1

    def measures_columns(self):
        #speed fluency
        #self.events['add_char_us_mess_dur'] = nm.nan
        #self.events['add_char_us_mess_dur'] = self.events['add_char_us_mess_dur'].replace(nm.nan,0)
        #self.events['add_char_us_turn_dur'] = nm.nan
        #self.events['add_char_us_turn_dur'] = self.events['add_char_us_turn_dur'].replace(nm.nan,0)
        #self.events['fin_char_us_mess_dur'] = nm.nan
        #self.events['fin_char_us_mess_dur'] = self.events['fin_char_us_mess_dur'].replace(nm.nan,0)
        #self.events['fin_char_us_turn_dur'] = nm.nan
        #self.events['fin_char_us_turn_dur'] = self.events['fin_char_us_turn_dur'].replace(nm.nan,0)
        self.events['words_mess_dur'] = nm.nan
        self.events['words_mess_dur'] = self.events['words_mess_dur'].replace(nm.nan,0)
        self.events['words_turn_dur'] = nm.nan
        self.events['words_turn_dur'] = self.events['words_turn_dur'].replace(nm.nan,0)
        #self.events['revisions_per_words'] = nm.nan
        #self.events['revisions_per_words'] = self.events['revisions_per_words'].replace(nm.nan,0)
        #self.events['revisions_mess_dur'] = nm.nan
        #self.events['revisions_mess_dur'] = self.events['revisions_mess_dur'].replace(nm.nan,0)
        #self.events['revisions_turn_dur'] = nm.nan
        #self.events['revisions_turn_dur'] = self.events['revisions_turn_dur'].replace(nm.nan,0)
        #self.events['revision_proportion'] = nm.nan
        #self.events['revision_proportion'] = self.events['revision_proportion'].replace(nm.nan,0)
        #self.events['mean_dur_r_bursts'] = nm.nan
        #self.events['mean_dur_r_bursts'] = self.events['mean_dur_r_bursts'].replace(nm.nan,0)
        self.events['mean_l_r_bursts'] = nm.nan
        self.events['mean_l_r_bursts'] = self.events['mean_l_r_bursts'].replace(nm.nan,0)
        self.events['mean_p_bursts'] = nm.nan
        self.events['mean_p_bursts'] = self.events['mean_p_bursts'].replace(nm.nan,0)
        self.events['pep'] = nm.nan
        self.events['pep'] = self.events['pep'].replace(nm.nan,0)
        self.events['mean_l_p_bursts'] = nm.nan
        self.events['mean_l_p_bursts'] = self.events['mean_l_p_bursts'].replace(nm.nan,0)
        self.events['mean_p_duration'] = nm.nan
        self.events['mean_p_duration'] = self.events['mean_p_duration'].replace(nm.nan,0)







    # we save the data in the test file
    def saveData(self):
        writer = self.events.to_csv("data.csv", sep=';')

    #we are getting the events
    def getEvents(self):
            return self.events


# we are doing the main
if __name__ == '__main__' :

    matches = eventLogLoader()
    matches.load('logs_events.json')
    matches.extractLengthDiff()
    matches.extractPauses()
    matches.revisions()
    matches.drop()
    matches.measures_columns()
    matches.saveData()
    print(matches.events.iloc[1])
    print(matches.events.iloc[1])

    res = matches.getEvents()
    print(res)
