#! venv/bin/python

"""

Use Twitter API to grab tweets using hashtags; 
export text file

Uses Twython module to access Twitter API

"""

import sys
import string
import simplejson #install simplejson at https://pypi.python.org/pypi/simplejson/
from twython import Twython #install Twython at https://github.com/ryanmcgrath/twython
import os
#WE WILL USE THE VARIABLES DAY, MONTH, AND YEAR FOR OUR OUTPUT FILE NAME
import datetime
import time
import subprocess

dir_map = {'Eastern':'east', 'Western':'west'}

def get_twitter_feed(t, name, hashtag, output_dir):
	#hashtag = 'Celtics' ##### this line need to change
	delimiter = ','
	data = t.search(q='#'+hashtag, count=100)
	tweets = data['statuses']

	#NAME OUR OUTPUT FILE - %i WILL BE REPLACED BY CURRENT MONTH, DAY, AND YEAR
	outfn = os.path.join(output_dir, name+"##"+hashtag+".csv")

	#NAMES FOR HEADER ROW IN OUTPUT FILE
	fields = "created_at text".split()

	#INITIALIZE OUTPUT FILE AND WRITE HEADER ROW   
	#outfp.write(string.join(fields, ",") + "\n")  # comment out if don't need header
	with open(outfn, "w") as outfp:
		for entry in tweets:
		   
			r = {}
			for f in fields:
				r[f] = ""
			#ASSIGN VALUE OF 'ID' FIELD IN JSON TO 'ID' FIELD IN OUR DICTIONARY
			r['created_at'] = entry['created_at']
			r['text'] = entry['text']
		
			print (r)
			#CREATE EMPTY LIST
			lst = []
			#ADD DATA FOR EACH VARIABLE
			for f in fields:
				lst.append(unicode(r[f]).replace("\/", "/"))
			#WRITE ROW WITH DATA IN LIST
			outfp.write(string.join(lst, delimiter).encode("utf-8") + "\n")
			
def get_team_data(input_file):
	team_data = dict()
	with open(input_file) as input_data:
		key = "No division"
		for line in input_data:
			if "Conference" in line and "Division" in line:
				key = "_".join(line.split())
				team_data[key] = []
			else:
				words = line.split()
				this_team = dict()
				this_team["name"] = ""
				for word in words:
					if "#" in word:
						this_team["hashtag"] = word.replace("#","").replace(",","")
					elif "@" in word:
						this_team["handle"] = word.replace("@","")
					else:
						this_team["name"] = this_team["name"] + word + " "
				team_data[key].append(this_team)
	return team_data
	 
	 
def get_all_csvs():
    now = int(time.time())
	#FOR OAUTH AUTHENTICATION -- NEEDED TO ACCESS THE TWITTER API
    t = Twython(app_key='qtW8Q4270j67gooVD19tvAGo9', #REPLACE 'APP_KEY' WITH YOUR APP KEY, ETC., IN THE NEXT 4 LINES
		app_secret='0Jnd7nIrLYv3BhZdiT98iKcaQKZBEipXziib0CitV2RZ6zXATQ',
		oauth_token='2652772872-W9GTB3c973ayomnFPW1qEFgieNpskT5yJAD0c29',
		oauth_token_secret='nHdpiqLHzQVGSfVqgM7JgFN89FSpdkJpLdTNX1YYskx0G')
    output_dir = "teamfeed-%s" % now

    if not os.path.exists(output_dir):
        os.mkdir(output_dir)
		
    team_data = get_team_data("team_data.txt")
    for dat in team_data:
        conference_dir = os.path.join(output_dir, dir_map[dat.split('_')[0]])
        if not os.path.exists(conference_dir):
            os.mkdir(conference_dir)
        for team in team_data[dat]:
            get_twitter_feed(t, team["name"].strip(), team["hashtag"], conference_dir)
			
if __name__=="__main__":
	get_all_csvs()
