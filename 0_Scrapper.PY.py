import pandas as pd
import requests
import urllib
import numpy as np

import os.path
from os import path

from bs4 import BeautifulSoup

from datetime import datetime
from datetime import timedelta

import time
import random

from tqdm import tqdm

import sys 


from os import listdir
from os.path import isfile, join


#General functions

#Function to produce unique match IDs
def numerise_string(x):

	alphabet = "abcdefghijklmnopqrstuvwxyz"
	tag = ""

	for letters in x:
		tag = tag + str(alphabet.find(letters.lower()))

	return tag


#Custom append function used for auto-save purposes
def update_frame(data, new_data):

	if len(data) == 0:
		return new_data
	else:
		return data.append(new_data)




def string_to_array(x):

		dummy = "abcdefghijklmnopqrstuvwxyz"
		alphabet = []
		for letters in dummy:
			alphabet.append(letters)	
		alphabet = np.array(alphabet)		

		arr = []

		for letters in x.lower():
			arr.append(np.where(alphabet == letters)[0][0])	

		return np.array(arr)


def match_two_names(x, y):

	x_arr = string_to_array(x)
	y_arr = string_to_array(y)

	n = len(x_arr)
	m = len(y_arr)
	n_try = m - n + 1

	if n_try > 0:
		match_p = []

		for i in range(0, n_try):
			v = x_arr - y_arr[0:n]
			match_p.append(len(np.where(v == 0)[0]) / n)
			y_arr = y_arr[1:]





class Baseball_Scrapper:

	def __init__(self, file_path):

		self.file_path = file_path
		dir_path = file_path + "/MLB_Modeling"

		#Create the repositories if they do not exist
		#Main repo
		target = file_path + "/MLB_Modeling"
		if not path.exists(target):
			os.mkdir(target)
			print("Main directory created at:" + "\t" + target)

		#Sub-repo
		sub_directories = ["Bat", "Pitch", "Scores", "Betting", "Misc"]
		for element in sub_directories:

			target = dir_path + "/" + element
			if not path.exists(target):
				os.mkdir(target)
				print("Sub-directory created at:" + "\t" + target)


		#Sub-repo locations
		self.paths = []
		for element in sub_directories:
			self.paths.append(dir_path + "/" + element)


		dictio_path = self.paths[4] + "/Abreviations_Dictionary.csv"
		if path.exists(dictio_path):
			self.dictio = pd.read_csv(dictio_path)
		else:
			self.dictio = []


		print("Scapper succesfully initiated.")


	#Updates a file, or save it if it doesn't exists
	def update_file(self, save_path, file_name, data):

		final_path = save_path + "/" + file_name

		try:
			if not path.exists(final_path):
				if len(data) > 0:
					data.to_csv(final_path, index = False)

			else:
				if len(data) > 0:
					pd.read_csv(final_path).append(data).drop_duplicates().to_csv(final_path, index = False)

		except:
			print("Failed to update file.")	



	#Translates a team name to its city name
	def Translate_Team_Names(self, value, want):

		if len(self.dictio) == 0:
			sys.exit("Missing file:" + "\t" + self.paths[4] + "Abreviations_Dictionary.csv")
		else:
			x = self.dictio

		m = len(x.columns)


		for j in range(0, m):
			location = np.where(x.iloc[:, j] == value)[0]

			if len(location) > 0:
				return x.at[location[0], want]


	#Frame-wide translate
	def Fix_Team_Names(self, frame, want):

		for col in frame.columns:

			if "Team" in col or "Opponent" in col:

				vector = np.array(list(frame[col]))
				values = np.array(list(set(vector)))

				m = np.where(frame.columns == col)[0][0]

				for team in values:

					index = np.where(vector == team)[0]
					proper_name = self.Translate_Team_Names(team, want)
					
					frame.iloc[index, m] = proper_name

		return frame




	###########################################################
	###########################################################
	######################## WORK FLOW ########################
	###########################################################
	###########################################################


	###########################################################
	#################### WEB SCRAPPING ########################
	###########################################################


	#Attempts to extract game URLs from a certain date
	#Is used inside a loop
	def Scrape_FanGraphs_game_url(self, date):

		url = "https://www.fangraphs.com/scoreboard.aspx?date=" + date

		html = requests.get(url).content
		html_content = BeautifulSoup(html, 'lxml')

		links = html_content.findAll('a')
		game_url = []

		for link in links:
			try:
				href = link.attrs['href']
			except:
				continue

			if "boxscore" in href:
				game_url.append("https://www.fangraphs.com/" + href)

		return game_url


	#Scrapes FanGraphs.com for urls to games that were played between to dates (frm, to)
	#Is used to initiate the database
	#Once done, the UPDATE_FanGraphs_Box_Scores method should be used
	def Get_FanGraphs_Game_URLs(self, frm, to):

		begin = datetime.strptime(frm, "%Y-%m-%d")
		end = datetime.strptime(to, "%Y-%m-%d")

		n = (end - begin).days + 1

		urls = pd.DataFrame(columns = ["URL"])
		no_games_dates = pd.DataFrame(columns = ["Dates"])
		games_dates = pd.DataFrame(columns = ["Dates"])


		#Check for dates which links were already scrapped
		if path.exists(self.paths[-1] + "/Game_Dates.csv"):
			dates_done = list(pd.read_csv(self.paths[-1] + "/Game_Dates.csv")["Dates"])
		else:
			dates_done = []

		#Main loop (extraction + auto-save)
		for i in tqdm(range(0, n)):

			date = datetime.strftime(begin, "%Y-%m-%d")

			#Avoid extracting for certain cases
			if (begin.month < 3) or (begin.month > 10) or (date in dates_done):
				begin = begin + timedelta(days = 1)
				continue

			#Retrieve links
			try:
				todays_url = self.Scrape_FanGraphs_game_url(date)
			except:
				no_games_dates = no_games_dates.append(pd.DataFrame(date, columns = ["Dates"]))
				begin = begin + timedelta(days = 1)
				continue

			if len(todays_url) > 0:
				urls = urls.append(pd.DataFrame(todays_url, columns = ["URL"]))
				games_dates = games_dates.append(pd.DataFrame([date], columns = ["Dates"])) 

				print("Scrapped:" + "\t" + date)


			#Saving procedure (trigerred every 20 iterations)
			if i % 20 == 0 or begin == end:

				self.update_file(self.paths[-1], "Game_URLs.csv", urls)
				urls = pd.DataFrame(columns = ["URL"])

				self.update_file(self.paths[-1], "No_Game_Dates.csv", no_games_dates)
				no_games_dates = pd.DataFrame(columns = ["Dates"])

				self.update_file(self.paths[-1], "Game_Dates.csv", games_dates)
				games_dates = pd.DataFrame(columns = ["Dates"])			

				print("Saved data.")


			begin = begin + timedelta(days = 1)
			time.sleep(random.randint(5, 10))

		print("Done.")



	#Get the Box Scores data based off a URL 
	def Scrape_FanGraphs_game_stats_by_url(self, ulr):

		html = requests.get(url).content
		tables = pd.read_html(html)

		#Date and team names
		url_split = url.split("-")
		date = url_split[0].split("=")[-1] + "-" + url_split[1] + "-" + url_split[2].split("&")[0]

		date_index = -1
		for table in tables:
			date_index += 1
			if table.iloc[0,0] == "Team":
				break	

		home_team = tables[date_index].iloc[2, 0]
		away_team = tables[date_index].iloc[1, 0]	

		#Score
		home_score = tables[date_index].iloc[2, -1]
		away_score = tables[date_index].iloc[1, -1]

		ID = ""
		temp = date.split("-")
		for values in temp:
			ID = ID + values

		ID = ID + numerise_string(home_team[0:2] + away_team[0:2])

		scores = pd.DataFrame(columns = ["Home", "Home_Score", "Away", "Away_Score", "Date", "URL", "ID"])
		scores.loc[0] = [home_team, home_score, away_team, away_score, date, url, ID]


		#Find where the extraction should begin
		start = 0
		for table in tables:
			start += 1
			if str(type(table.columns)) == "<class 'pandas.core.indexes.multi.MultiIndex'>":
				break

		tables = tables[start:]

		#Find the play by play table
		table_lengths = []
		for table in tables:
			table_lengths.append(len(table))

		table_lengths = np.array(table_lengths)

		play_by_play_index = np.where(table_lengths == np.max(table_lengths))[0][0]
		play_by_play = tables[play_by_play_index]
		del tables[play_by_play_index]
		table_lengths = np.delete(table_lengths, play_by_play_index)

		#Merge the frames
		merged_tables = []
		for i in range(0, 4):

			temp_table = tables[i]
			for j in range(4, len(tables)):

				size = len(temp_table)

				if len(tables[j]) == size:

					check = len(np.where(tables[i]["Name"] == tables[j]["Name"])[0])
					if check == size:

						temp_table = pd.merge(temp_table, tables[j], on = "Name")

			temp_table["Date"] = date
			if i % 2 == 0:
				temp_table["Team"] = home_team
				temp_table["Location"] = "Home"
				temp_table["Opponent"] = away_team
			else:
				temp_table["Team"] = away_team
				temp_table["Location"] = "Away"
				temp_table["Opponent"] = home_team

			colnames = []
			for j in range(0, len(temp_table.columns)):
				colnames.append(temp_table.columns[j].split("_")[0])

			temp_table.columns = colnames
			temp_table["ID"] = ID

			merged_tables.append(temp_table.loc[:,~temp_table.columns.duplicated()])




		merged_tables.append(scores)

		return merged_tables



	#Extracts the box scores based off the URL list
	def Extract_FanGraphs_Box_Scores(self):

		url_path = self.paths[-1] + "/Game_URLs.csv"
		if path.exists(url_path):

			urls = list(set(list(pd.read_csv(url_path)["URL"])))


			#Checks for existing Box_Scores
			path_to_check = self.paths[2] + "/FanGraphs_Scores.csv"
			if path.exists(path_to_check):
				urls_done = list(pd.read_csv(path_to_check).drop_duplicates()["URL"])

				urls = [x for x in urls if x not in urls_done]


			#Initialise variables

			bat = []
			pitch = []
			scores = []

			count = 0
			n = len(urls)

			print("Extracting " + str(n) + " Box Scores...")
			#e_time = round((((45/2) + 3) * n) / 60, 2)
			#print("Estimated running time:" + "\t" + str(e_time) + " minutes")

			#Loop throught URLs 
			for i in tqdm(range(0, n)):

				url = str(urls[i])
				count += 1
				try:
					tables = self.Scrape_FanGraphs_game_stats_by_url(url)
				except:
					time.sleep(random.randint(5,10))
					continue

				bat = update_frame(bat, tables[0].append(tables[1]))
				pitch = update_frame(pitch, tables[2].append(tables[3]))
				scores = update_frame(scores, tables[4])

				print("\t" + "\t" + "\t" + "***** ADDED GAME *****")
				print(scores.iloc[-1,:])

				#print(scores)

				if count % 20 == 0 or url == urls[-1]:

					self.update_file(self.paths[0], "FanGraphs_Box_Scores.csv", bat)	
					bat = []

					self.update_file(self.paths[1], "FanGraphs_Box_Scores.csv", pitch)	
					pitch = []					

					self.update_file(self.paths[2], "FanGraphs_Scores.csv", scores)	
					scores = []

					print("\t" + "\t" + "\t" + "***** PROGRESS SAVED *****")

				if url != urls[-1]:
					time.sleep(random.randint(3, 7))




	###########################################################
	#################### UPDATE CODES  ########################
	###########################################################


	#MAIN FUNCTION
	#Scrapes within the interval [last_scrapped, today]
	#Update the Box_Scores
	#Clean the data if needed
	def UPDATE_FanGraphs_Box_Scores(self):

		path_check = self.paths[2] + "/FanGraphs_Scores.csv"
		if not path.exists(path_check):
			sys.exit("Missing file:" + "\t" + path_check)

		frm = pd.read_csv(path_check)["Date"].sort_values("Date",ascending=False)["Date"]
		n = len(frm)
		frm = frm[0]
		to = datetime.strftime(to, "%Y-%m-%d")

		self.Get_FanGraphs_Game_URLs(frm, to)
		self.Extract_FanGraphs_Box_Scores()

		n_new = len(pd.read_csv(path_check))
		if n_new > n:
			self.Clean_Data()
		else:
			print("No new Box Scores to scrape.")



	#Extracts the box scores based off the URL list
	def Extract_FanGraphs_Box_Scores_FROM_MISSING_MATCHES(self):

		url_path = self.paths[-1] + "/Missing_Matches.csv"
		if path.exists(url_path):

			file_missing_urls = pd.read_csv(url_path)

			urls = list(set(list(file_missing_urls["URL"])))

			#Checks for existing Box_Scores
			path_to_check = self.paths[2] + "/FanGraphs_Scores.csv"
			if path.exists(path_to_check):
				urls_done = list(pd.read_csv(path_to_check).drop_duplicates()["URL"])

				urls = [x for x in urls if x not in urls_done]


			#Initialise variables

			bat = []
			pitch = []
			scores = []

			count = 0
			n = len(urls)

			print("Extracting " + str(n) + " Box Scores...")
			#e_time = round((((45/2) + 3) * n) / 60, 2)
			#print("Estimated running time:" + "\t" + str(e_time) + " minutes")

			#Loop throught URLs 
			for i in tqdm(range(0, n)):

				url = str(urls[i])
				count += 1
				try:
					tables = self.Scrape_FanGraphs_game_stats_by_url(url)
				except:
					time.sleep(random.randint(5,10))
					continue

				bat = update_frame(bat, tables[0].append(tables[1]))
				pitch = update_frame(pitch, tables[2].append(tables[3]))
				scores = update_frame(scores, tables[4])

				print("\t" + "\t" + "\t" + "***** ADDED GAME *****")
				print(scores.iloc[-1,:])

				#print(scores)

				if count % 20 == 0 or url == urls[-1]:

					self.update_file(self.paths[0], "FanGraphs_Box_Scores.csv", bat)	
					bat = []

					self.update_file(self.paths[1], "FanGraphs_Box_Scores.csv", pitch)	
					pitch = []					

					self.update_file(self.paths[2], "FanGraphs_Scores.csv", scores)	
					scores = []

					print("\t" + "\t" + "\t" + "***** PROGRESS SAVED *****")

				if url != urls[-1]:
					time.sleep(random.randint(3, 7))


	###########################################################
	#################### DATA CLEANING  #######################
	###########################################################

	#Cleans the bat, pitch and scores frames
	def Clean_Data(self):

		#Create sub-repositories if they do not already exist
		sufix = "/Clean_Data"

		for i in range(0, (len(self.paths) - 1)):
			path_string = self.paths[i] + sufix
			if not path.exists(path_string):
				os.mkdir(path_string)
				print("Create sub-directory at:" + "\t" + path_string)

		scores_path = self.paths[2] + "/FanGraphs_Scores.csv"
		if not path.exists(scores_path):
			sys.exit("No data to clean.")
		else:
			scores = pd.read_csv(scores_path)

		scores.columns = ["Team_Home", "Score_Home", "Team_Away", "Score_Away", "Date", "URL", "ID"]

		#Load bat and pitch frames
		frames = []
		for i in range(0,2):

			path_string = self.paths[i] + "/FanGraphs_Box_Scores.csv"
			if not path.exists(path_string):
				sys.exit("Missing file:" + "\t" + path_string)
			else:
				frames.append(pd.read_csv(path_string, dtype={'a': str})) 


		#Use CITY abreviations for TEAMS
		scores = self.Fix_Team_Names(scores, "City")
		for i in range(0,2):
			frames[i] = self.Fix_Team_Names(frames[i], "City")


		#Find double-matches days
		IDs = np.array(list(scores["ID"]))
		doubles = [x for x in IDs if len(np.where(IDs == x)[0]) > 1]

		if len(doubles) > 0:

			fix = list(set(list(doubles)))

			m = np.where(scores.columns == "ID")[0][0]

			for values in fix:

				index_scores = np.where(IDs == values)[0][1]

				for i in range(0, 2):

					index = np.where(frames[i]["ID"] == values)[0]
					temp_names = frames[i].iloc[index, :]["Name"]

					split = np.where(temp_names == "Total")[0][1] + 1
					to_replace = index[split:]

					col_index = np.where(frames[i].columns == "ID")[0][0]

					frames[i].iloc[to_replace, col_index] = -values


				scores.iloc[index_scores, m] = -values


		#Tag starting pitchers
		frames[1]["Starting"] = "No"
		IDs = list(scores["ID"])
		for i in tqdm(range(0, len(IDs))):

			ID = IDs[i]
			index_match = np.where(frames[1]["ID"] == ID)[0]
			if len(index_match) == 0:
				continue

			teams = list(set(list(frames[1]["Team"][index_match])))
			for team in teams:
				starting = index_match[np.where(frames[1]["Team"][index_match] == team)[0][0]]
				frames[1].at[starting, "Starting"] = "Yes"



		for i in range(0, 2):

			x = frames[i]

			#Remove "Total" rows
			rmv = np.where(x["Name"] == "Total")[0]
			x = x.drop(rmv).reset_index(drop = True)


			#The are NaNs due to ratios
			#Create dummy columns for NaNs

			n_NaNs = x.isna().sum()
			fix = np.where(n_NaNs > 0)[0]
			cols_to_fix = x.columns[fix]

			if len(fix) > 0:
				for cnames in cols_to_fix:

					#Replace with 0
					col_index = np.where(x.columns == cnames)[0][0]
					to_replace = np.where(x[cnames].isna())[0]

					if "%" in cnames or cnames == "HR/FB":
						x.iloc[to_replace, col_index] = "0.0%"
					else:
						if x[cnames].dtype == np.float64:
							x.iloc[to_replace, col_index] = 0.0
						else:
							x.iloc[to_replace, col_index] = 0

					#Add dummy column
					new_name = cnames + "_NaN"
					x[new_name] = 0

					col_index = np.where(x.columns == new_name)[0][0]
					x.iloc[to_replace, col_index] = 1


			#Format percentages
			data_types = list(x.dtypes)
			for j in range(0, len(x.columns)):
				if data_types[j] == np.float64 or data_types[j] == np.int64:
					continue
				
				else:
					m = x.columns[j]

					if ("%" in m and not "NaN" in m) or m == "HR/FB":
						try:
							x[m] = x[m].str.replace("%", "").astype(float) / 100
						except:
							problem = [k for k, x in enumerate(list(x[m])) if "," in x]	
							index_col = np.where(x.columns == m)[0][0]
							x.iloc[problem, index_col] = "0.0%"

							x[m] = x[m].str.replace("%", "").astype(float) / 100

					else:
						try:
							x[m] = x[m].astype(float)
						except:
							continue


			#Fix column_names
			colnames = list(x.columns)
			for j in range(0, len(colnames)):

				if colnames[j][0] == "-":
					colnames[j] = colnames[j][1:] + "_minus"
				elif x.columns[j][0] == "+":
					colnames[j] = colnames[j][1:] + "_plus"		

			x.columns = colnames		


			#Add position variable
			#Only for bat
			try:

				splitted_names = pd.DataFrame(list(x["Name"].str.split(" - ")), columns = ["Name", "Position"])

				x["Name"] = (splitted_names["Name"] + " " +  x["Team"]).str.replace(" ", "")

				temp = list(set(list(splitted_names["Position"])))

				positions = list()
				for values in temp:

					y = values.split("-")
					for vals in y:
						if not vals in positions:
							positions.append(vals)

				position_names = []

				for values in positions:
					c_name = "Position_" + values
					x[c_name] = 0
					position_names.append(c_name)


				for j in range(0, len(x)):

					y = splitted_names["Position"][j].split("-")
					for values in y:
						c_name = "Position_" + values
						x.at[j, c_name] = 1

				frames[i] = x.sort_values("Date", ascending=False)

			except:

				splitted_names = pd.DataFrame(list(x["Name"].str.split("(")), columns = ["Name", "Position"])
				x["Name"] = (splitted_names["Name"] + " " +  x["Team"]).str.replace(" ", "")
				frames[i] = x.sort_values("Date", ascending=False)


		scores = scores.sort_values("Date", ascending = False)
		for i in range(0, 2):
			frames[i] = frames[i].sort_values("Date", ascending = False)

		#Save the cleaned files
		for i in range(0, 2):
			save_path = self.paths[i] + "/Clean_Data/FanGraphs_Box_Scores.csv"
			frames[i].to_csv(save_path, index = False)
			print("Saved:" + "\t" + save_path)

		save_path = self.paths[2] + "/Clean_Data/FanGraphs_Scores.csv"
		scores = scores.sort_values("Date", ascending=False)
		scores.to_csv(save_path, index = False)
		print("Saved:" + "\t" + save_path)


		print("Cleaning done.")


	#Cleans betting data
	def Clean_Betting_Data(self):

		#Set sub-directory up
		path_data = self.paths[3] + "/Clean_Data"
		if not path.exists(path_data):
			os.mkdir(path_data)
			print("Created sub-directory at:" + "\t" + path_data)

		#Extract CSV files if needed
		url = "https://www.sportsbookreviewsonline.com/scoresoddsarchives/mlb/mlboddsarchives.htm"
		html = requests.get(url).content
		html_content = BeautifulSoup(html, 'lxml')
		links = html_content.findAll('a')

		file_url = []
		for link in links:
			try:
				href = link.attrs['href']
			except:
				continue

			if ".xlsx" in href:
				file_url.append(str("https://www.sportsbookreviewsonline.com/scoresoddsarchives/mlb/" + href))


		for x in file_url:

			year = x.split("%")[-1].split(".")[0][2:]
			path_save = self.paths[3] + "/MLB_Odds_" + str(year) + ".csv"
			if not path.exists(path_save):

				file = pd.read_excel(x)
				file.to_csv(path_save, index = False)
				print("Downloaded:" + "\t" + path_save)




		FG_teams = []
		FG_teams = np.array(FG_teams)
		all_teams = np.array(list(set(list(FG_teams))))
		team_index = []
		for teams in all_teams:
			team_index.append(np.where(FG_teams == teams)[0])


		#Format the files
		frame = []

		for i in tqdm(range(2010, (datetime.now().year + 1))):

			path_check = self.paths[3] + "/MLB_Odds_" + str(i) + ".csv"
			if path.exists(path_check):

				temp = pd.read_csv(path_check).reset_index(drop = True)

				#Fix dates
				temp["Date"] = temp["Date"].astype(str)

				for j in range(0, len(temp)):
					u = temp.at[j, "Date"]
					if len(temp.at[j, "Date"]) == 3:
						temp.at[j, "Date"] = str(i) + "-" + "0" + u[0] + "-" + u[1:]
					else:
						temp.at[j, "Date"] = str(i) + "-" + u[0:2] + "-" + u[2:]


				#Convert moneyline values to returns
				moneylines = ["Open", "Close"]

				rmv = np.where(temp["Open"] == "NL")[0]
				if len(rmv) > 0:
					temp = temp.drop(rmv)
					temp = temp.reset_index(drop = True)
					temp["Open"] = temp["Open"].astype(int)
					temp["Close"] = temp["Close"].astype(int)

				for vals in moneylines:

					m = np.where(temp.columns == vals)[0][0]

					index = np.where(temp[vals] > 0)[0]
					temp.iloc[index, m] = temp.iloc[index, m] / 100

					index = np.where(temp[vals] < 0)[0]
					temp.iloc[index, m] = 100 / (- temp.iloc[index, m])									


				split_frames = []
				values = ["H", "V"]
				temp = temp[["Date", "Team", "Open", "Close", "VH", "Final", "Pitcher"]]

				temp["Pitcher"] = temp["Pitcher"].str.replace("-L", "").str.replace("-R", "")
				for j in range(0, len(temp)):
					temp.at[j, "Pitcher"] = str(temp.at[j, "Pitcher"])[1:]

				#Translate team names
				temp = self.Fix_Team_Names(temp, "City")
				temp = temp.reset_index(drop = True)


				for vals in values:

					index = np.where(temp["VH"] == vals)[0]
					split_frames.append(temp.iloc[index, :])
					del split_frames[-1]["VH"]

					if vals == "H":
						split_frames[-1].columns = ["Date", "Team_Home", "Open_Home", "Close_Home", "Score_Home", "Pitcher_Home"]
					else:
						split_frames[-1].columns = ["Date", "Team_Away", "Open_Away", "Close_Away", "Score_Away", "Pitcher_Away"]
						del split_frames[-1]["Date"]

					split_frames[-1] = split_frames[-1].reset_index(drop = True)

				#Assemble
				temp = pd.concat(split_frames, axis = 1)

				#Compute implied odds
				temp["Open_Winning_Odds_Home"] = 1 / (1 + temp["Open_Home"])
				temp["Close_Winning_Odds_Home"] = 1 / (1 + temp["Close_Home"])

				temp["Open_Winning_Odds_Away"] = 1 / (1 + temp["Open_Away"])
				temp["Close_Winning_Odds_Away"] = 1 / (1 + temp["Close_Away"])

				temp["Open_Winning_Odds_Home"] = temp["Open_Winning_Odds_Home"] / (temp["Open_Winning_Odds_Home"] + temp["Open_Winning_Odds_Away"])
				temp["Close_Winning_Odds_Home"] = temp["Close_Winning_Odds_Home"] / (temp["Close_Winning_Odds_Home"] + temp["Close_Winning_Odds_Away"])

				temp["Open_Winning_Odds_Away"] = 1 - temp["Open_Winning_Odds_Home"]
				temp["Close_Winning_Odds_Away"] = 1 - temp["Close_Winning_Odds_Home"]


				if len(frame) == 0:
					frame = temp
				else:
					frame = frame.append(temp)


		frame = frame.iloc[::-1]
		frame = frame.reset_index(drop = True)


		#Attempt to add IDs
		path_scores = self.paths[2] + "/Clean_Data/FanGraphs_Scores.csv"
		if path.exists(path_scores):

			print("\t" + "\t" + "\t" + "***** Adding IDs *****")

			frame["ID"] = 0
			scores = pd.read_csv(path_scores)

			for i in tqdm(range(0, len(scores))):

				a = np.where(frame["Date"] == scores.at[i, "Date"])[0]
				if len(a) == 0:
					continue

				b = np.where(frame["Team_Home"][a] == scores.at[i, "Team_Home"])[0]
				if len(b) == 0:
					continue

				a = a[b]

				b = np.where(frame["Score_Home"][a] == scores.at[i, "Score_Home"])[0]
				if len(b) == 0:
					continue

				a = a[b]

				b = np.where(frame["Score_Away"][a] == scores.at[i, "Score_Away"])[0]
				if len(b) == 0:
					continue

				index = a[b]

				if len(index) > 0:
					frame.at[index[0], "ID"] = scores.at[i, "ID"]


		rmv = np.where(frame["ID"] == 0)[0]
		if len(rmv) > 0:
			frame = frame.drop(rmv)


		frame.to_csv(self.paths[3] + "/Clean_Data/MLB_Odds.csv", index = False)
		print("\t" + "\t" + "***** MLB Moneyline data successfully formated *****")



	def Build_Filling_Pitchers_Database(self):

		frame_path = self.paths[1] + "/Clean_Data/FanGraphs_Box_Scores.csv"
		if not path.exists(frame_path):
			sys.exit("Missing file:" + "\t" + frame_path)

		frame = pd.read_csv(frame_path)
		
		index = np.where(frame["Starting"] == "No")[0]
		frame.loc[index, "Name"] = "ReliefPitcher" + frame.iloc[index]["Name"].str[-3:].copy()

		frame.to_csv(self.paths[1] + "/Clean_Data/FanGraphs_Box_Scores_SP.csv", index = False)




	#################################################################################
	#################### MATCH STATS AVERAGE QUERY FUNCTION  ########################
	#################################################################################


	#Calculate average statistics given a list of per-game statistics of a rooster
	def Combine_Players_BAT(self, frame, scale):

		to_sum = ["BO", "PA", "H", "HR", "R", "RBI", "BB", "SO", "IBB", 
					"1B", "2B", "3B", "HBP", "SF", "SH", "GDP", "SB", "CS",
						"GB", "FB", "LD", "IFFB", "IFH", "BU", "BUH", "Balls",
							"Strikes", "Pitches", "WPA", "WPA_plus", "WPA_minus", "PH",
								"pLI", "AB", "wRC+", "Spd", "wSB", "wRC", "RE24"]

		summed_stats = pd.DataFrame(frame[to_sum].sum()).transpose()

		summed_stats["LI"] =  ((frame["WPA"] / frame["WPA/LI"]).replace([np.inf, -np.inf], np.nan)).sum()
		summed_stats["BIP"] =  summed_stats["AB"] - summed_stats["SO"] - summed_stats["HR"] + summed_stats["SF"]

		wOBA_divisors = frame["AB"] + frame["BB"] - frame["IBB"] + frame["SF"] + frame["HBP"]
		summed_stats["wOBA"] = (frame["wOBA"] * wOBA_divisors).sum() / wOBA_divisors.sum()

		summed_stats["BB%"] = summed_stats["BB"] / summed_stats["PA"]
		summed_stats["SO%"] = summed_stats["SO"] / summed_stats["PA"]

		summed_stats["ISO"] = (summed_stats["2B"] + 2*summed_stats["3B"] + 3*summed_stats["HR"]) / summed_stats["AB"]
		summed_stats["BABIP"] = (summed_stats["H"] - summed_stats["HR"]) / (summed_stats["AB"] - summed_stats["SO"] - summed_stats["HR"] + summed_stats["SF"])
		summed_stats["OBP"] = (summed_stats["H"] + summed_stats["BB"] + summed_stats["HBP"]) / (summed_stats["AB"] + summed_stats["BB"] + summed_stats["HBP"] + summed_stats["SF"])
		summed_stats["SLG"] = (summed_stats["1B"] + 2*summed_stats["2B"] + 3*summed_stats["3B"] + 4*summed_stats["HR"]) / summed_stats["AB"]

		summed_stats["BB/SO"] = summed_stats["BB"] / summed_stats["SO"]
		summed_stats["OPS"] = summed_stats["OBP"] + summed_stats["SLG"]

		summed_stats["wRAA"] = (frame["wRAA"] * frame["PA"]).sum() / summed_stats["PA"]
		summed_stats["GB/FB"] = summed_stats["GB"] / summed_stats["FB"]

		summed_stats["LD%"] = summed_stats["LD"] / summed_stats["BIP"]
		summed_stats["FB%"] = summed_stats["FB"] / summed_stats["BIP"]
		summed_stats["GB%"] = summed_stats["GB"] / summed_stats["BIP"]
		summed_stats["IFFB%"] = summed_stats["IFFB"] / summed_stats["FB"]

		summed_stats["HR/FB"] = summed_stats["HR"] / summed_stats["FB"]
		summed_stats["IFH%"] = summed_stats["IFH"] / summed_stats["GB"]

		temp = frame["BUH%"].copy()
		fix = np.where(temp == 0)[0]
		if len(fix) > 0:
		    temp.iloc[fix] = 1

		summed_stats["BUH%"] = summed_stats["BUH"] / ((1 / temp) * frame["BUH"]).sum()
		summed_stats["REW"] = summed_stats["RE24"] / (frame["RE24"] / frame["REW"]).sum()

		summed_stats["WPA/LI"] = summed_stats["WPA"] / summed_stats["LI"]
		summed_stats["WPA/pLI"] = summed_stats["WPA"] / summed_stats["pLI"]
		summed_stats["Clutch"] = summed_stats["WPA/LI"] - summed_stats["WPA/pLI"]

		summed_stats = summed_stats.fillna(0)

		if scale:

			to_sum = to_sum + ["LI", "BIP"]
			summed_stats[to_sum] = summed_stats[to_sum] / len(frame)

		return summed_stats.replace([np.inf, -np.inf], np.nan).fillna(0)



	def Combine_Players_PITCH(self, frame, scale):

		to_sum = ["IP", "TBF", "H", "HR", "ER", "BB", "SO", "pLI", "BS",
					"GS", "G", "CG", "ShO", "SV", "HLD", "R", "IBB", "IFH",
						"HBP", "WP", "BK", "WPA", "LD", "FB", "GB", "BUH", "tERA",
							"IFFB", "BU", "RS", "Balls", "Strikes", "Pitches",
								"WPA_plus", "WPA_minus", "RE24", "inLI", "gmLI", "exLI"]		

		summed_stats = pd.DataFrame(frame[to_sum].sum()).transpose()
		summed_stats["BIP"] =  (round(1 / frame["BABIP"] * (frame["H"] - frame["HR"]))).sum()
		summed_stats["LI"] =  ((frame["WPA"] / frame["WPA/LI"]).replace([np.inf, -np.inf], np.nan)).sum()


		#fip_constant = (frame["FIP"] - (13*frame["HR"] + 3*(frame["BB"] + frame["HBP"]) - 2*frame["SO"]) / frame["IP"]).mean()
		#summed_stats["FIP"] = (13*summed_stats["HR"] + 3*(summed_stats["BB"] + summed_stats["HBP"]) - 2*summed_stats["SO"]) / summed_stats["IP"] + fip_constant
		summed_stats["FIP"] = (13*summed_stats["HR"] + 3*(summed_stats["BB"] + summed_stats["HBP"]) - 2*summed_stats["SO"]) / summed_stats["IP"]

		summed_stats["xFIP"] = summed_stats["FIP"] + (frame["FIP"] - frame["xFIP"]).mean()

		try:
			dFip_minus_over_dFip = round((frame["FIP-"].diff() / frame["FIP"].diff()).mean())

		except:
			dFip_minus_over_dFip = 22

		beta_1 = (frame["FIP-"] - dFip_minus_over_dFip*frame["FIP"]).mean()
		summed_stats["FIP-"] = beta_1 + dFip_minus_over_dFip * summed_stats["FIP"] 


		summed_stats["K/9"] = 9 * summed_stats["SO"] / summed_stats["IP"]
		summed_stats["BB/9"] = 9 * summed_stats["BB"] / summed_stats["IP"]
		summed_stats["HR/9"] = 9 * summed_stats["HR"] / summed_stats["IP"]

		summed_stats["K/BB"] = summed_stats["K/9"] / summed_stats["BB/9"]

		summed_stats["HR/FB"] = summed_stats["HR"] / summed_stats["FB"]

		summed_stats["ERA"] = 9*summed_stats["ER"] / summed_stats["IP"]

		summed_stats["BABIP"] = (summed_stats["H"] - summed_stats["HR"]) / summed_stats["BIP"]

		summed_stats["LOB%"] = (summed_stats["H"] + summed_stats["BB"] + summed_stats["HBP"] - summed_stats["R"]) / (summed_stats["H"] + summed_stats["BB"] + summed_stats["HBP"] - 1.4*summed_stats["HR"])
		
		summed_stats["LD%"] = summed_stats["LD"] / summed_stats["BIP"]
		summed_stats["FB%"] = summed_stats["FB"] / summed_stats["BIP"]
		summed_stats["GB%"] = summed_stats["GB"] / summed_stats["BIP"]
		
		summed_stats["BB%"] = summed_stats["BB"] / summed_stats["TBF"]
		summed_stats["K%"] = summed_stats["SO"] / summed_stats["TBF"]
		summed_stats["K-BB%"] = summed_stats["K%"] - summed_stats["BB%"]

		summed_stats["IFFB%"] = summed_stats["IFFB"] / summed_stats["FB"]
		summed_stats["IFH%"] = summed_stats["IFH"] / summed_stats["GB"]
		summed_stats["BUH%"] = summed_stats["BUH"] / summed_stats["BU"]

		summed_stats["HR/FB"] = summed_stats["HR"] / summed_stats["FB"]
		summed_stats["ERA"] = 9*summed_stats["ER"] / summed_stats["IP"]

		summed_stats["WPA/LI"] = summed_stats["WPA"] / summed_stats["LI"]
		summed_stats["WPA/pLI"] = summed_stats["WPA"] / summed_stats["pLI"]
		summed_stats["Clutch"] = summed_stats["WPA/LI"] - summed_stats["WPA/pLI"]		

		summed_stats["LOB%"] = (summed_stats["H"] + summed_stats["BB"] + summed_stats["HBP"] - summed_stats["R"]) / (summed_stats["H"] + summed_stats["BB"] + summed_stats["HBP"] - 1.4 * summed_stats["HR"])

		if scale:

			to_sum = to_sum + ["LI", "BIP"]
			summed_stats[to_sum] = summed_stats[to_sum] / len(frame)

		return summed_stats.replace([np.inf, -np.inf], np.nan).fillna(0)


	def Compute_Seasonal_Averages(self):

		path_check = self.paths[0].split("/Bat")[0] + "/Regression"
		if not path.exists(path_check):
			os.mkdir(path_check)
			print("Created directory at:" + "\t" + path_check)

		frames = []
		for i in range(0, 2):
			path_check = self.paths[i] + "/Clean_Data/FanGraphs_Box_Scores.csv"
			if not path.exists(path_check):
				sys.exit("Missing file at:" + "\t" + path_check)

			frames.append(pd.read_csv(path_check))	

		path_check = self.paths[2] + "/Clean_Data/FanGraphs_Scores.csv"
		if not path.exists(path_check):
			os.mkdir(path_check)
			print("Created directory at:" + "\t" + path_check)

		scores = pd.read_csv(path_check)


		years = scores["Date"].str[:4].astype("int")
		seasons = np.sort(list(set(list(years))))
		teams = list(set(list(scores["Team_Home"])))

		averages_bat = []
		averages_pitch = []

		for season in seasons:

			temp_bat = []
			temp_pitch = []

			temp = np.where(years == season)[0]

			ids = scores.loc[temp, "ID"].copy().reset_index(drop = True)
			teams_home = scores.loc[temp, "Team_Home"].copy().reset_index(drop = True)


			for team in tqdm(teams):

				ids_home = ids[np.where(teams_home == team)[0]]

				for i in range(0,2):

					index = np.where(np.isin(frames[i]["ID"], ids_home))[0]

					if i == 0:
						temp_bat.append(self.Combine_Players_BAT(frames[i].iloc[index], True))

					else:
						temp_pitch.append(self.Combine_Players_PITCH(frames[i].iloc[index], True))


			temp_bat = pd.concat(temp_bat)
			temp_pitch = pd.concat(temp_pitch)

			temp = [temp_bat, temp_pitch]
			for i in range(0, 2):
				temp[i]["Season"] = season
				temp[i]["Team"] = teams

			averages_bat.append(temp[0])
			averages_pitch.append(temp[1])


		averages_bat = pd.concat(averages_bat)
		averages_bat = averages_bat.reset_index(drop = True)

		averages_pitch = pd.concat(averages_pitch)
		averages_pitch = averages_pitch.reset_index(drop = True)
		
		path_save = self.paths[0].split("/Bat")[0] + "/Regression"

		averages_bat.to_csv(path_save + "/Seasonal_Averages_Bat.csv", index = False)
		averages_pitch.to_csv(path_save + "/Seasonal_Averages_Pitch.csv", index = False)



	def Expected_Values_X(self, last_n_days):

		#Load the list of matches that were processed
		path_check = self.paths[0].split("/Bat")[0] + "/Regression/" + str(last_n_days) + "/X.csv"
		if not path.exists(path_check):
			sys.exit("Missing file:" + "\t" + path_check)

		processed_matches = pd.read_csv(path_check)[['Date', 'Team_Home', 'Team_Away', 'ID']]


		#Check if the averages are already computed.
		path_check = self.paths[0].split("/Bat")[0] + "/Regression/" + str(last_n_days) + "/X_Averages.csv"
		if path.exists(path_check): 
			IDs_done = pd.read_csv(path_check)["ID"]

			to_flush = np.where(np.isin(processed_matches["ID"], IDs_done))[0]
			if len(to_flush) > 0:

				processed_matches = processed_matches.drop(to_flush).reset_index(drop = True)
				print(str(len(to_flush)) + " matches were already processed...")
				print("Updating...")


		#Load the averages
		averages = []
		names = ["Bat.csv", "Pitch.csv"]
		for name in names:

			path_check = self.paths[0].split("/Bat")[0] + "/Regression/Seasonal_Averages_" + name
			if not path.exists(path_check):
				sys.exit("Missing file:" + "\t" + path_check)	

			averages.append(pd.read_csv(path_check))


		#Load the rosters
		frames = []
		for i in range(0, 2):
			path_check = self.paths[i] + "/Clean_Data/FanGraphs_Box_Scores.csv"
			if not path.exists(path_check):
				sys.exit("Missing file at:" + "\t" + path_check)

			if i == 0:
				temp = pd.read_csv(path_check)[["Name", "Date", "Team", "Opponent", "Location", "ID"]]
			else:
				temp = pd.read_csv(path_check)[["Name", "Date", "Team", "Opponent", "Location", "ID", "IP", "Starting"]]

			temp["Played_at"] = temp["Team"].copy()
			index = np.where(temp["Location"] == "Away")[0]
			temp.loc[index, "Played_at"] = temp.loc[index, "Opponent"].copy()


			frames.append(temp)	


		path_check = self.paths[1] + "/Clean_Data/FanGraphs_Box_Scores_SP.csv"
		if not path.exists(path_check):
			sys.exit("Missing file at:" + "\t" + path_check)

		temp = pd.read_csv(path_check)[["Name", "Date", "Team", "Opponent", "Location", "ID", "IP", "Starting"]]
		temp["Played_at"] = temp["Team"].copy()
		index = np.where(temp["Location"] == "Away")[0]
		temp.loc[index, "Played_at"] = temp.loc[index, "Opponent"].copy()

		frames.append(temp)	
		del temp


		date_vectors = list()
		for i in range(0, 2):
			date_vectors.append(pd.to_datetime(frames[i]["Date"]))

		rows = []
		location = ["Home", "Away"]

		#Compute averages
		count = 0
		for ID, dates in tqdm(zip(list(processed_matches["ID"]), list(processed_matches["Date"])), total = len(list(processed_matches["ID"]))):
			count += 1

			date = pd.to_datetime(dates)
			to = date - timedelta(days = 1)
			frm = date - timedelta(days = last_n_days)


			#Obtain expected value frames
			season = to.year - 1
			if season < np.min(averages[0]["Season"]):
				continue

			avgs = list()
			for i in range(0, 2):
				avgs.append(averages[i].iloc[np.where(averages[i]["Season"] == season)[0]].copy().reset_index(drop = True))


			temp_frames = list()
			for i in range(0, 2):
				index = np.where(frames[i]["ID"] == ID)[0]
				temp_frames.append(frames[i].iloc[index].copy().reset_index(drop = True))


			lnd_frames = list()
			for i in range(0, 2):

				index = np.where((date_vectors[i] > frm) & (date_vectors[i] <= to))[0]
				temp = frames[i].iloc[index].copy().reset_index(drop = True)

				index = np.where(np.isin(temp, temp_frames[i]["Name"]))[0]
				lnd_frames.append(temp.iloc[index].copy().reset_index(drop = True))
				del temp


			#Compute expected values

			bat_temp = []
			pitch_temp = []

			for loc in location:

				#Bat
				index = np.where(temp_frames[0]["Location"] == loc)[0]
				names = temp_frames[0].loc[index, "Name"].copy().reset_index(drop = True)

				index = np.where(np.isin(lnd_frames[0]["Name"], names))[0]
				weight_frame = lnd_frames[0].loc[index, "Played_at"].copy().reset_index(drop = True)

				a = list(weight_frame)
				b = list(avgs[0]["Team"])

				index = [b.index(x) for x in a]
				bat_frame = self.Combine_Players_BAT(avgs[0].iloc[index], True)

				cnames = list(bat_frame.columns)
				for j in range(0, len(cnames)):
					cnames[j] = cnames[j].replace("%", "_Percent") + "_Bat_" + loc 

				bat_frame.columns = cnames
				bat_temp.append(bat_frame)


				#Pitch
				index = np.where(temp_frames[1]["Location"] == loc)[0]
				names = temp_frames[1].loc[index, ["Name", "Starting"]].copy().reset_index(drop = True)		

				starting_pitcher = names.loc[np.where(names["Starting"] == "Yes")[0][0], "Name"]

				index = np.where(np.isin(lnd_frames[1]["Name"], starting_pitcher))[0]
				weight_frame = lnd_frames[1].loc[index, ["Played_at", "Starting", "IP"]].copy().reset_index(drop = True)	

				starting_index = np.where(weight_frame["Starting"] == "Yes")[0]
				if len(starting_index) > 1:
					weight_frame = weight_frame.iloc[starting_index].reset_index(drop = True)

				everywhere_played = lnd_frames[1].iloc[np.where(lnd_frames[1]["Team"] == starting_pitcher[-3:])[0]].drop_duplicates(subset = "ID", keep = "last").reset_index(drop = True)
				everywhere_played = everywhere_played["Played_at"]

				starting_played = weight_frame["Played_at"]

				a = list(starting_played)
				b = list(avgs[1]["Team"])
				starter = self.Combine_Players_PITCH(avgs[1].iloc[[b.index(x) for x in a]], True)

				a = list(everywhere_played)
				relief = self.Combine_Players_PITCH(avgs[1].iloc[[b.index(x) for x in a]], False)

				IP_per_game = 8.425209861450691
				weight_filler = 1 - starter["IP"] / IP_per_game
				IP_filler = weight_filler * IP_per_game
				scale_filler = float(IP_filler / relief["IP"])	
				
				relief = scale_filler * relief	
				pitch_overall = self.Combine_Players_PITCH(pd.concat([relief, starter]), scale = True) 

				cnames = list(pitch_overall.columns)
				for j in range(0, len(cnames)):
					cnames[j] = cnames[j].replace("%", "_Percent") + "_Pitch_" + loc 

				pitch_overall.columns = cnames
				pitch_temp.append(pitch_overall)


			out = pd.concat([bat_temp[0], pitch_temp[0], bat_temp[1], pitch_temp[1]], axis = 1)
			out["ID"] = ID

			rows.append(out)

			#Save
			if ((count % 150 == 0) or (count == len(processed_matches))) and (len(rows) > 0):

				print("Saving progress...")
				to_save = pd.concat(rows).reset_index(drop = True)
				save_at = self.paths[0].split("/Bat")[0] + "/Regression/" + str(last_n_days) 
				self.update_file(save_at, "X_Averages.csv", to_save)

				rows = []



	def Query_X_row_centered(self, pitcher_home, batters_home, pitcher_away, batters_away, date, last_n_days, at_location, bat, pitch, pitchSP, averages):

		location = ["Home", "Away"]
		batters = [batters_home, batters_away]
		pitchers = [pitcher_home, pitcher_away]
		pitcher_fillers = [["ReliefPitcher" + pitcher_home[0][-3:]], ["ReliefPitcher" + pitcher_away[0][-3:]]]

		out = []
		out_average = []
		for i in range(0,2):

			#Retrieve batters with a time-filter (last_n_days)
			index_bat = np.where(bat["Name"].isin(batters[i]))[0]
			if len(index_bat) == 0:
				sys.exit("Error: No batters found..")				

			dates = pd.to_datetime(bat["Date"][index_bat].copy())
			to = pd.to_datetime(date) - timedelta(days = 1)
			frm = pd.to_datetime(date) - timedelta(days = last_n_days)

			#Obtain expected value frames
			season = to.year - 1
			if season < np.min(averages[0]["Season"]):
				sys.exit("No averages to center...")

			avgs = list()
			for j in range(0, 2):
				avgs.append(averages[j].iloc[np.where(averages[j]["Season"] == season)[0]].copy().reset_index(drop = True))

			
			index_bat = index_bat[np.where((dates > frm) & (dates <= to))[0]]
			if len(index_bat) < (4 * len(batters[i])):
				sys.exit("Error: No enough matches found for batters.")			

			bat_temp = bat.iloc[index_bat].copy().reset_index(drop = True)


			#Minimum n = 4 * number of batters queried
			if location == True:
				index = np.where(bat_temp["Location"] == location[i])[0]
				if len(index) < (4 * len(batters[i])):
					sys.exit("Error: Not enough matches played by batters at " + location[i] + ".")

				bat_temp = bat_temp.iloc[index].reset_index(drop = True)

			#Compute expected values
			bat_temp["Played_at"] = bat_temp["Team"].copy()
			index = np.where(bat_temp["Location"] == "Away")[0]
			bat_temp.loc[index, "Played_at"] = bat_temp.loc[index, "Opponent"].copy()

			a = list(bat_temp["Played_at"])
			b = list(avgs[0]["Team"])

			index = [b.index(x) for x in a]
			bat_average = self.Combine_Players_BAT(avgs[0].iloc[index], True)
			cnames = list(bat_average.columns)
			for j in range(0, len(cnames)):

				cnames[j] = cnames[j].replace("%", "_Percent") + "_Bat_" + location[i] 

			bat_average.columns = cnames

			#Compute individual averages
			overall_rows = [] 
			for name in batters[i]:

				index = np.where(bat_temp["Name"] == name)[0]
				if len(index) == 0:
					continue

				overall_rows.append(self.Combine_Players_BAT(bat_temp.iloc[index], scale = True))

			#At least 6 batters must be found within the database to proceed
			if len(overall_rows) <= 6:
				sys.exit("Error: Not enough batters found.")

			#Compute the average of the individual averages
			#(Individuals with few matches played won't be penalized)
			overall_bat = self.Combine_Players_BAT(pd.concat(overall_rows), scale = True) 
			cnames = list(overall_bat.columns)
			for j in range(0, len(cnames)):

				cnames[j] = cnames[j].replace("%", "_Percent") + "_Bat_" + location[i] 

			overall_bat.columns = cnames


			#Retrieve the starting picher with a time-filter (last_n_days)
			index_pitch = np.where(pitch["Name"].isin(pitchers[i]))[0]
			if len(index_pitch) == 0:
				sys.exit("Error: No pitchers found.")				

			dates = pd.to_datetime(pitch["Date"][index_pitch].copy())

			#Minimum n = 2
			index_pitch = index_pitch[np.where((dates > frm) & (dates <= to))[0]]
			if len(index_pitch) < 2:
				sys.exit("Error: Not enough matches found for the pitcher.")				

			pitch_temp = pitch.iloc[index_pitch].copy().reset_index(drop = True)

			#Attempt to select only matches where the pitcher was starting
			#Minimum n = 2
			starting_index = np.where(pitch_temp["Starting"] == "Yes")[0]
			if len(starting_index) > 1:
				pitch_temp = pitch.iloc[index_pitch].copy().reset_index(drop = True)

			pitch_overall_starting = self.Combine_Players_PITCH(pitch_temp, scale = True) 


			#Compute expected values
			pitch_temp["Played_at"] = pitch_temp["Team"].copy()
			index = np.where(pitch_temp["Location"] == "Away")[0]
			pitch_temp.loc[index, "Played_at"] = pitch_temp.loc[index, "Opponent"].copy()

			a = list(pitch_temp["Played_at"])
			b = list(avgs[1]["Team"])
			starter_average = self.Combine_Players_PITCH(avgs[1].iloc[[b.index(x) for x in a]], True)	



			#Retrieve the filling pichers with a time-filter (last_n_days)
			index_pitch = np.where(pitchSP["Name"].isin(pitcher_fillers[i]))[0]
			if len(index_pitch) == 0:
				sys.exit("Error: No filling pitchers found.")				

			dates = pd.to_datetime(pitch["Date"][index_pitch].copy())

			index_pitch = index_pitch[np.where((dates > frm) & (dates <= to))[0]]
			if len(index_pitch) == 0:
				sys.exit("Error: No filling pitchers found within date range.")				
				

			#Compute unscalled averages of filling pitchers per match	
			pitch_temp = pitchSP.iloc[index_pitch].copy().reset_index(drop = True)
			pitch_fill_rows = []
			for ID in list(set(list(pitch_temp["ID"]))):
				index = np.where(pitch_temp["ID"] == ID)[0]
				pitch_fill_rows.append(self.Combine_Players_PITCH(pitch_temp.iloc[index], scale = False))

			#Combine the unscalled filling pitches averages into a single scaled row
			pitch_overall_filling = self.Combine_Players_PITCH(pd.concat(pitch_fill_rows), scale = True) 


			#Compute expected values
			pitch_temp["Played_at"] = pitch_temp["Team"].copy()
			index = np.where(pitch_temp["Location"] == "Away")[0]
			pitch_temp.loc[index, "Played_at"] = pitch_temp.loc[index, "Opponent"].copy()

			pitch_temp_unique = pitch_temp.drop_duplicates(subset = "ID", keep = "last").reset_index(drop = True)

			a = list(pitch_temp_unique["Played_at"])
			relief_average = self.Combine_Players_PITCH(avgs[1].iloc[[b.index(x) for x in a]], True)


			#Compute a weighted average of the starting and filling pitchers
			#IP_per_game constant was calculated from the 2010 to 2019 seasons as sum(IP) / (2 * number of matches)
			IP_per_game = 8.425209861450691
			weight_filler = 1 - pitch_overall_starting["IP"] / IP_per_game
			IP_filler = weight_filler * IP_per_game
			scale_filler = float(IP_filler / pitch_overall_filling["IP"])

			#Adjust the filling pitcher frame
			pitch_overall_filling = scale_filler * pitch_overall_filling

			pitch_overall = pd.concat([pitch_overall_starting, pitch_overall_filling])
			pitch_overall = self.Combine_Players_PITCH(pitch_overall, scale = True) 
			cnames = list(pitch_overall.columns)
			for j in range(0, len(cnames)):

				cnames[j] = cnames[j].replace("%", "_Percent") + "_Pitch_" + location[i] 

			pitch_overall.columns = cnames

			#Adjust means
			starter_average = starter_average * float(pitch_overall_starting["IP"]) / float(starter_average["IP"])
			relief_average = relief_average * float(pitch_overall_filling["IP"]) / float(relief_average["IP"])

			pitch_average = self.Combine_Players_PITCH(pd.concat([starter_average, relief_average]), scale = True) 
			cnames = list(pitch_average.columns)
			for j in range(0, len(cnames)):

				cnames[j] = cnames[j].replace("%", "_Percent") + "_Pitch_" + location[i] 

			pitch_average.columns = cnames		


			out.append(pd.concat([overall_bat - bat_average, pitch_overall - pitch_average], axis=1))


		return pd.concat(out, axis=1)



	def Query_X_row(self, pitcher_home, batters_home, pitcher_away, batters_away, date, last_n_days, at_location, bat, pitch, pitchSP):

		location = ["Home", "Away"]
		batters = [batters_home, batters_away]
		pitchers = [pitcher_home, pitcher_away]
		pitcher_fillers = [["ReliefPitcher" + pitcher_home[0][-3:]], ["ReliefPitcher" + pitcher_away[0][-3:]]]

		out = []
		for i in range(0,2):

			#Retrieve batters with a time-filter (last_n_days)
			index_bat = np.where(bat["Name"].isin(batters[i]))[0]
			if len(index_bat) == 0:
				sys.exit("Error: No batters found..")				

			dates = pd.to_datetime(bat["Date"][index_bat].copy())
			to = pd.to_datetime(date) - timedelta(days = 1)
			frm = pd.to_datetime(date) - timedelta(days = last_n_days)

			index_bat = index_bat[np.where((dates > frm) & (dates <= to))[0]]
			if len(index_bat) < (4 * len(batters[i])):
				sys.exit("Error: No enough matches found for batters.")			

			bat_temp = bat.iloc[index_bat].copy().reset_index(drop = True)

			#Minimum n = 4 * number of batters queried
			if location == True:
				index = np.where(bat_temp["Location"] == location[i])[0]
				if len(index) < (4 * len(batters[i])):
					sys.exit("Error: Not enough matches played by batters at " + location[i] + ".")

				bat_temp = bat_temp.iloc[index].reset_index(drop = True)


			#Compute individual averages
			overall_rows = [] 
			for name in batters[i]:

				index = np.where(bat_temp["Name"] == name)[0]
				if len(index) == 0:
					continue

				overall_rows.append(self.Combine_Players_BAT(bat_temp.iloc[index], scale = True))

			#At least 6 batters must be found within the database to proceed
			if len(overall_rows) <= 6:
				sys.exit("Error: Not enough batters found.")

			#Compute the average of the individual averages
			#(Individuals with few matches played won't be penalized)
			overall_bat = self.Combine_Players_BAT(pd.concat(overall_rows), scale = True) 
			cnames = list(overall_bat.columns)
			for j in range(0, len(cnames)):

				cnames[j] = cnames[j].replace("%", "_Percent") + "_Bat_" + location[i] 

			overall_bat.columns = cnames


			#Retrieve the starting picher with a time-filter (last_2n_days)
			index_pitch = np.where(pitch["Name"].isin(pitchers[i]))[0]
			if len(index_pitch) == 0:
				sys.exit("Error: No pitchers found.")				

			dates = pd.to_datetime(pitch["Date"][index_pitch].copy())

			#Minimum n = 1
			frm = pd.to_datetime(date) - timedelta(days = 2*last_n_days)
			index_pitch = index_pitch[np.where((dates > frm) & (dates <= to))[0]]
			if len(index_pitch) < 1:
				sys.exit("Error: Not enough matches found for the pitcher.")				

			pitch_temp = pitch.iloc[index_pitch].copy().reset_index(drop = True)	

			#Attempt to select only matches where the pitcher was starting
			#Minimum n = 1
			starting_index = np.where(pitch_temp["Starting"] == "Yes")[0]
			if len(starting_index) > 0:
				pitch_temp = pitch.iloc[index_pitch].copy().reset_index(drop = True)

			pitch_overall_starting = self.Combine_Players_PITCH(pitch_temp, scale = True) 


			#Retrieve the filling pichers with a time-filter (last_n_days)
			frm = pd.to_datetime(date) - timedelta(days = last_n_days)
			index_pitch = np.where(pitchSP["Name"].isin(pitcher_fillers[i]))[0]
			if len(index_pitch) == 0:
				sys.exit("Error: No filling pitchers found.")				

			dates = pd.to_datetime(pitch["Date"][index_pitch].copy())

			index_pitch = index_pitch[np.where((dates > frm) & (dates <= to))[0]]
			if len(index_pitch) == 0:
				sys.exit("Error: No filling pitchers found within date range.")				
				

			#Compute unscalled averages of filling pitchers per match	
			pitch_temp = pitchSP.iloc[index_pitch].copy().reset_index(drop = True)
			pitch_fill_rows = []
			for ID in list(set(list(pitch_temp["ID"]))):
				index = np.where(pitch_temp["ID"] == ID)[0]
				pitch_fill_rows.append(self.Combine_Players_PITCH(pitch_temp.iloc[index], scale = False))

			#Combine the unscalled filling pitches averages into a single scaled row
			pitch_overall_filling = self.Combine_Players_PITCH(pd.concat(pitch_fill_rows), scale = True) 	

			#Compute a weighted average of the starting and filling pitchers
			#IP_per_game constant was calculated from the 2010 to 2019 seasons as sum(IP) / (2 * number of matches)
			IP_per_game = 8.425209861450691
			weight_filler = 1 - pitch_overall_starting["IP"] / IP_per_game
			IP_filler = weight_filler * IP_per_game
			scale_filler = float(IP_filler / pitch_overall_filling["IP"])

			#Adjust the filling pitcher frame
			pitch_overall_filling = scale_filler * pitch_overall_filling

			pitch_overall = pd.concat([pitch_overall_starting, pitch_overall_filling])
			pitch_overall = self.Combine_Players_PITCH(pitch_overall, scale = True) 
			cnames = list(pitch_overall.columns)
			for j in range(0, len(cnames)):

				cnames[j] = cnames[j].replace("%", "_Percent") + "_Pitch_" + location[i] 

			pitch_overall.columns = cnames
			out.append(pd.concat([overall_bat, pitch_overall], axis=1))


		return pd.concat(out, axis=1)




	##############################################################
	#################### REGRESSION FRAME  #######################
	##############################################################


	def Query_from_ID(self, ID, last_n_days, at_location, bat, pitch, pitchSP):

		index_bat = np.where(bat["ID"] == ID)[0]
		index_pitch = np.where(pitch["ID"] == ID)[0]
		index_pitchSP = np.where(pitchSP["ID"] == ID)[0]

		date = str(bat.iloc[index_bat[0]]["Date"])

		home = np.where(bat.iloc[index_bat]["Location"] == "Home")[0]
		away = np.where(bat.iloc[index_bat]["Location"] == "Away")[0]

		batters_home = list(bat.iloc[index_bat[home]]["Name"])
		batters_away = list(bat.iloc[index_bat[away]]["Name"])

		home = np.where((pitch.iloc[index_pitch]["Location"] == "Home") & (pitch.iloc[index_pitch]["Starting"] == "Yes"))[0]
		away = np.where((pitch.iloc[index_pitch]["Location"] == "Away") & (pitch.iloc[index_pitch]["Starting"] == "Yes"))[0]	

		pitcher_home = list(pitch.iloc[index_pitch[home]]["Name"])
		pitcher_away = list(pitch.iloc[index_pitch[away]]["Name"])

		return self.Query_X_row(pitcher_home, batters_home, pitcher_away, batters_away, date, last_n_days, at_location, bat, pitch, pitchSP)
	

	def Query_from_ID_centered(self, ID, last_n_days, at_location, bat, pitch, pitchSP, averages):

		index_bat = np.where(bat["ID"] == ID)[0]
		index_pitch = np.where(pitch["ID"] == ID)[0]
		index_pitchSP = np.where(pitchSP["ID"] == ID)[0]

		date = str(bat.iloc[index_bat[0]]["Date"])

		home = np.where(bat.iloc[index_bat]["Location"] == "Home")[0]
		away = np.where(bat.iloc[index_bat]["Location"] == "Away")[0]

		batters_home = list(bat.iloc[index_bat[home]]["Name"])
		batters_away = list(bat.iloc[index_bat[away]]["Name"])

		home = np.where((pitch.iloc[index_pitch]["Location"] == "Home") & (pitch.iloc[index_pitch]["Starting"] == "Yes"))[0]
		away = np.where((pitch.iloc[index_pitch]["Location"] == "Away") & (pitch.iloc[index_pitch]["Starting"] == "Yes"))[0]	

		pitcher_home = list(pitch.iloc[index_pitch[home]]["Name"])
		pitcher_away = list(pitch.iloc[index_pitch[away]]["Name"])

		return self.Query_X_row_centered(pitcher_home, batters_home, pitcher_away, batters_away, date, last_n_days, at_location, bat, pitch, pitchSP, averages)
	



	def Query_all_MLB_Odds_matches(self, last_n_days, at_location, purge):

		path_check = self.paths[0].split("/Bat")[0] + "/Regression"
		if not path.exists(path_check):
			os.mkdir(path_check)
			print("Created directory at:" + "\t" + path_check)

		path_check = path_check + "/" + str(last_n_days)
		if not path.exists(path_check):
			os.mkdir(path_check)
			print("Created directory at:" + "\t" + path_check)		


		path_check = self.paths[3] + "/Clean_Data/MLB_Odds.csv"
		if not path.exists(path_check):
			sys.exit("Missing file at:" + "\t" + path_check)

		scores = pd.read_csv(path_check)

		frames = []
		for i in range(0, 2):
			path_check = self.paths[i] + "/Clean_Data/FanGraphs_Box_Scores.csv"
			if not path.exists(path_check):
				sys.exit("Missing file at:" + "\t" + path_check)

			frames.append(pd.read_csv(path_check))	


		path_check = self.paths[1] + "/Clean_Data/FanGraphs_Box_Scores_SP.csv"
		if not path.exists(path_check):
			sys.exit("Missing file at:" + "\t" + path_check)

		frames.append(pd.read_csv(path_check))	


		#Purges IDs that were previously processed if the directory associated with "last_n_days" already exists
		path_check = self.paths[0].split("/Bat")[0] + "/Regression" + "/" + str(last_n_days) + "/X.csv"
		if path.exists(path_check):
			print("Regression frame already exists, updating...")

			temp_frame = pd.read_csv(path_check)			
			IDs_done = list(temp_frame["ID"])

			if purge:
				top_date = min(pd.to_datetime(temp_frame["Date"])) 
				to_rmv = np.where(pd.to_datetime(scores["Date"]) > top_date)[0]
				IDs_done = list(set(list(IDs_done + list(scores.loc[to_rmv, "ID"]))))

			keep = np.where(scores["ID"].isin(IDs_done) == False)[0]
			if len(keep) == 0:
				sys.exit("No matches to process.")
			scores = scores.iloc[keep].reset_index(drop = True)

			if purge:

				for i in range(0, len(frames)):
					keep = np.where(frames[i]["ID"].isin(IDs_done) == False)[0]
					if len(keep) == 0:
						sys.exit("No matches to process.")
					frames[i] = frames[i].iloc[keep].reset_index(drop = True)

		rows_out_x = []

		for i in tqdm(range(0, len(scores))):

			ID = scores.at[i, "ID"]

			try:
				new_row = self.Query_from_ID(ID, last_n_days, at_location, frames[0], frames[1], frames[2])

			except:
				print("\t" + "\t" + "**** Insuficient data for match ****:")
				print(pd.DataFrame(scores.iloc[i]).transpose()[["Date", "Team_Home", "Team_Away"]])
				continue

			if len(new_row) > 0:

				to_add = pd.DataFrame(scores.iloc[i]).transpose().copy().reset_index(drop = True)
				new_row[to_add.columns] = to_add

				rows_out_x.append(new_row)

			else:
				print("\t" + "\t" + "**** Insuficient data for match ****:")
				print(pd.DataFrame(scores.iloc[i]).transpose()[["Date", "Team_Home", "Team_Away"]])				

			#Save and purge every 100 iterations
			if ((i + 1) % 100 == 0 or i == (len(scores) - 1)) and len(rows_out_x) > 0:

				#Assemble rows into a frame and save
				X = pd.concat(rows_out_x)	
				path_check = self.paths[0].split("/Bat")[0] + "/Regression" + "/" + str(last_n_days) 					
				self.update_file(path_check, "X.csv", X)

				IDs_done = list(X["ID"])

				rows_out_x = []
					
				if purge:

					for i in range(0, len(frames)):
						keep = np.where(frames[i]["ID"].isin(IDs_done) == False)[0]
						if len(keep) == 0:
							sys.exit("No matches to process.")
						frames[i] = frames[i].iloc[keep].reset_index(drop = True)

				print("Purged frame and saved progress.")



	def Query_all_MLB_Odds_matches_centered(self, last_n_days, at_location, purge):

		#Load the averages
		averages = []
		names = ["Bat.csv", "Pitch.csv"]
		for name in names:

			path_check = self.paths[0].split("/Bat")[0] + "/Regression/Seasonal_Averages_" + name
			if not path.exists(path_check):
				sys.exit("Missing file:" + "\t" + path_check)	

			averages.append(pd.read_csv(path_check))


		path_check = self.paths[0].split("/Bat")[0] + "/Regression"
		if not path.exists(path_check):
			os.mkdir(path_check)
			print("Created directory at:" + "\t" + path_check)

		path_check = path_check + "/" + str(last_n_days)
		if not path.exists(path_check):
			os.mkdir(path_check)
			print("Created directory at:" + "\t" + path_check)		


		path_check = self.paths[3] + "/Clean_Data/MLB_Odds.csv"
		if not path.exists(path_check):
			sys.exit("Missing file at:" + "\t" + path_check)

		scores = pd.read_csv(path_check)

		frames = []
		for i in range(0, 2):
			path_check = self.paths[i] + "/Clean_Data/FanGraphs_Box_Scores.csv"
			if not path.exists(path_check):
				sys.exit("Missing file at:" + "\t" + path_check)

			frames.append(pd.read_csv(path_check))	


		path_check = self.paths[1] + "/Clean_Data/FanGraphs_Box_Scores_SP.csv"
		if not path.exists(path_check):
			sys.exit("Missing file at:" + "\t" + path_check)

		frames.append(pd.read_csv(path_check))	


		#Purges IDs that were previously processed if the directory associated with "last_n_days" already exists
		path_check = self.paths[0].split("/Bat")[0] + "/Regression" + "/" + str(last_n_days) + "/X_Centered.csv"
		if path.exists(path_check):
			print("Regression frame already exists, updating...")

			temp_frame = pd.read_csv(path_check)			
			IDs_done = list(temp_frame["ID"])

			if purge:
				top_date = min(pd.to_datetime(temp_frame["Date"])) 
				to_rmv = np.where(pd.to_datetime(scores["Date"]) > top_date)[0]
				IDs_done = list(set(list(IDs_done + list(scores.loc[to_rmv, "ID"]))))

			keep = np.where(scores["ID"].isin(IDs_done) == False)[0]
			if len(keep) == 0:
				sys.exit("No matches to process.")
			scores = scores.iloc[keep].reset_index(drop = True)

			if purge:

				for i in range(0, len(frames)):
					keep = np.where(frames[i]["ID"].isin(IDs_done) == False)[0]
					if len(keep) == 0:
						sys.exit("No matches to process.")
					frames[i] = frames[i].iloc[keep].reset_index(drop = True)

		rows_out_x = []

		for i in tqdm(range(0, len(scores))):

			ID = scores.at[i, "ID"]

			try:
				new_row = self.Query_from_ID_centered(ID, last_n_days, at_location, frames[0], frames[1], frames[2], averages)

			except:
				print("\t" + "\t" + "**** Insuficient data for match ****:")
				print(pd.DataFrame(scores.iloc[i]).transpose()[["Date", "Team_Home", "Team_Away"]])
				continue

			if len(new_row) > 0:

				to_add = pd.DataFrame(scores.iloc[i]).transpose().copy().reset_index(drop = True)
				new_row[to_add.columns] = to_add

				rows_out_x.append(new_row)

			else:
				print("\t" + "\t" + "**** Insuficient data for match ****:")
				print(pd.DataFrame(scores.iloc[i]).transpose()[["Date", "Team_Home", "Team_Away"]])				

			#Save and purge every 100 iterations
			if ((i + 1) % 100 == 0 or i == (len(scores) - 1)) and len(rows_out_x) > 0:

				#Assemble rows into a frame and save
				X = pd.concat(rows_out_x)	
				path_check = self.paths[0].split("/Bat")[0] + "/Regression" + "/" + str(last_n_days) 					
				self.update_file(path_check, "X_Centered.csv", X)

				IDs_done = list(X["ID"])

				rows_out_x = []
					
				if purge:

					for i in range(0, len(frames)):
						keep = np.where(frames[i]["ID"].isin(IDs_done) == False)[0]
						if len(keep) == 0:
							sys.exit("No matches to process.")
						frames[i] = frames[i].iloc[keep].reset_index(drop = True)

				print("Purged frame and saved progress.")



	########################################################################
	#################### INDIVIDUAL PLAYER DATABASE  #######################
	########################################################################

	#Give each player their own file
	def Build_Individual_Players_Database(self):

		#Get information needed to tag starting and relief pitchers
		path_needed = self.paths[3] + "/Clean_Data/MLB_Odds.csv"
		if not path.exists(path_needed):
			sys.exit("Missing file:" + "\t" + path_needed)

		scores = pd.read_csv(path_needed)
		scores_IDs = np.array(list(scores["ID"]))

		for i in range(0,2):

			frame_path = self.paths[i] + "/Clean_Data/FanGraphs_Box_Scores.csv"
			if not path.exists(frame_path):
				sys.exit("Missing file:" + "\t" + frame_path)

			else:
				frame = pd.read_csv(frame_path)

				names = list(set(list(frame["Name"])))

				#Create sub-directory for by_player database
				sub_dir = self.paths[i] + "/Clean_Data/By_Player"
				if not path.exists(sub_dir):
					os.mkdir(sub_dir)
					print("Created sub-directory at:" + "\t" + sub_dir) 

				x = np.array(frame["Name"])

				rmv = []
				for name in tqdm(names):

					index = np.where(x == name)[0]
					temp_path = sub_dir + "/" + name + ".csv"

					if i == 0:
						frame.iloc[index, :].to_csv(temp_path, index = False)

					#Tag starting vs relief pitchers
					else:
						temp = frame.iloc[index, :].copy()
						temp = temp.reset_index(drop = True)
						temp["Starting"] = "No"
						IDs_temp = np.array(list(temp["ID"]))
						location_temp = list(temp["Location"])
						for j in range(0, len(temp)):
							k = np.where(scores_IDs == IDs_temp[j])[0]
							if len(k) == 0:
								continue
							else:
								k = k[0]
								if location_temp[j] == "Home":
									starting_pitcher = str(scores.at[k ,"Pitcher_Home"])
								else:
									starting_pitcher = str(scores.at[k ,"Pitcher_Away"])

								if starting_pitcher == name:
									temp.at[j, "Starting"] = "Yes"

						temp.to_csv(temp_path, index = False)

					rmv = rmv + list(index)

					if len(rmv) > 25000:

						frame = frame.drop(rmv)
						frame = frame.reset_index(drop = True)
						x = np.delete(x, rmv)
						rmv = []

			print("Per-Player Database built at:" + "\t" + sub_dir)



	def Classify_Individual_Players_Database(self):

		#Set-up directories
		for i in range(0,2):

			values = ["Home", "Away"]
			values_pitch = ["Starting", "Relief"]

			for j in range(0,2):
				sub_dir = self.paths[i] + "/Clean_Data/By_Player" + "/" + values[j]
				if not path.exists(sub_dir):
					os.mkdir(sub_dir)
					print("Created sub-directory at:" + "\t" + sub_dir)

				if i == 1:
					for k in range(0,2):
						sub_dir_2 = sub_dir + "/" + values_pitch[k]
						if not path.exists(sub_dir_2):
							os.mkdir(sub_dir_2)
							print("Created sub-directory at:" + "\t" + sub_dir_2)						


		#Classify data between home and away
		for i in range(0,2):
			path_target = self.paths[i] + "/Clean_Data/By_Player"

			all_files = [f for f in listdir(path_target) if isfile(join(path_target, f))]
			all_files = [f for f in all_files if ".csv" in f]

			for file in tqdm(all_files):
				temp = pd.read_csv(path_target + "/" + file).reset_index(drop = True)

				home_index = np.where(temp["Location"] == "Home")[0]
				if len(home_index > 0):
					temp.iloc[home_index].to_csv(path_target + "/Home/" + file)

					if i == 1:
						temp_2 = temp.iloc[home_index].reset_index(drop = True)						
						starting_index = np.where(temp_2["Starting"] == "Yes")[0]
						if len(starting_index) > 0:
							temp_2.iloc[starting_index].to_csv(path_target + "/Home/Starting/" + file)

						relief_index = np.where(temp_2["Starting"] == "No")[0]
						if len(relief_index) > 0:
							temp_2.iloc[relief_index].to_csv(path_target + "/Home/Relief/" + file)					

				away_index = np.where(temp["Location"] == "Away")[0]
				if len(away_index > 0):
					temp.iloc[away_index].to_csv(path_target + "/Away/" + file)

					if i == 1:
						temp_2 = temp.iloc[away_index].reset_index(drop = True)						
						starting_index = np.where(temp_2["Starting"] == "Yes")[0]
						if len(starting_index) > 0:
							temp_2.iloc[starting_index].to_csv(path_target + "/Away/Starting/" + file)

						relief_index = np.where(temp_2["Starting"] == "No")[0]
						if len(relief_index) > 0:
							temp_2.iloc[relief_index].to_csv(path_target + "/Away/Relief/" + file)						



	#Compute players' individual average stats for the previous n-games
	def Build_Individual_Players_ROLLING_AVERAGE_Database(self, n):

		for i in range(0,2):
			
			#Build directories
			sufix = "/Clean_Data/By_Player/Rolling_Averages"
			path_check = self.paths[i] + sufix
			if not path.exists(path_check):
				os.mkdir(path_check)
				print("Created sub-directory at:" + "\t" + path_check)

			sufix = sufix + "/" + str(n)
			path_check = self.paths[i] + sufix
			if not path.exists(path_check):
				os.mkdir(path_check)
				print("Created sub-directory at:" + "\t" + path_check)		


			#Build the database
			path_folder = self.paths[i] + sufix
			path_players = self.paths[i] + "/Clean_Data/FanGraphs_Box_Scores.csv"

			if not path.exists(path_players):
				sys.exit("Missing file:" + "\t" + path_players)

			players = list(set(list(pd.read_csv(path_players)["Name"])))

			for player in tqdm(players):
				path_player = self.paths[i] + "/Clean_Data/By_Player/" + player + ".csv"
				if not path.exists(path_player):
					continue

				data = pd.read_csv(path_player)

				data_numeric = data.select_dtypes(exclude = "O")
				data_string = data.select_dtypes(include = "O")
				data_ID = list(data["ID"])[:-1]

				new_cols = []

				n_iter = len(data) - 1
				for j in range(0, n_iter):

					frm = j + 1
					to = np.min([j + n, len(data) - 1])

					new_cols.append(list(data_numeric.loc[frm:to, :].copy().mean()))

				data_string = data_string.drop(len(data_string) - 1).reset_index(drop = True)
				data_numeric = pd.DataFrame(data = new_cols, columns = data_numeric.columns)

				data = pd.concat([data_string, data_numeric], axis = 1)
				data["ID"] = data_ID

				path_save = path_check +"/" + player + ".csv"
				data.to_csv(path_save, index = False)



#####################################################
#################### EXAMPLE  #######################
#####################################################


frame = pd.read_csv("D:/MLB/MLB_Modeling/Pitch/Clean_Data/By_Player/A.J.AchterANA.csv")

file_path = "D:/MLB"
n = 7
self = Baseball_Scrapper(file_path)

#Update
self.UPDATE_FanGraphs_Box_Scores()

#Clean
self.Clean_Data()
self.Clean_Betting_Data()

#Build player database
self.Build_Individual_Players_Database()
self.Build_Individual_Players_ROLLING_AVERAGE_Database(n)

#Build regression frame
self.Prepare_Regression_Frames(n)



