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
			if (i + 1) % 20 == 0 or begin == end:

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

				if (count + 1) % 20 == 0 or url == urls[-1]:

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

		temp = pd.read_csv(path_check)
		n = len(temp)

		frm = temp["Date"].max()
		to = datetime.strftime(datetime.now(), "%Y-%m-%d")

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


	##############################################################################
	#################### PREDICTED LINEUPS AND MONEYLINES  #######################
	##############################################################################

	def Scrape_Predicted_Lineups(self):

		headers = {
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36',
    	}

		date = datetime.strftime(datetime.now(), "%Y-%m-%d")

		print("Accessing https://www.lineups.com/mlb/lineups ...")

		#Extract projected roosters
		html = requests.get("https://www.lineups.com/mlb/lineups" , stream = True, headers = headers).content
		tables = pd.read_html(html)
		soup = BeautifulSoup(html)


		print("Extracting tables ...")
		#Extract full player names
		players_hrefs = soup.find_all("a", class_ = "link-black-underline")
		players = []
		family_names = []
		for element in players_hrefs:
			if "player-stats" in element["href"]:

				temp = element["href"].split("/")[-1].split("-")
				out = ""
				for x in temp:
					out = out + x.capitalize()

				players.append(out)

				if temp[-1].capitalize() == "Jr":
					family_names.append(temp[-2].capitalize() + temp[-1].capitalize())
				else:
					family_names.append(temp[-1].capitalize())


		family_names = np.array(family_names)

		#Extract moneylines
		moneylines = soup.find_all("div", class_ = "lineup-foot-stat-col")

		#Extract the date
		temp = soup.find("span", class_ = "hidden-xs-down").text.split(" ")[-1].split("/")
		for i in range(0, len(temp)):
			if len(temp[i]) == 1:
				temp[i] = "0" + temp[i]
		date = str(2000 + int(temp[2])) + "-" + temp[0] + "-" + temp[1]



		#Build tables in loop
		bat = []
		pitch = []
		teams = []

		print("Formating tables ...")

		n = int(len(tables) / 2)
		for i in range(0, n):

			#Get batters
			batters = pd.DataFrame(tables[2*i]).iloc[:,0].copy()
			for j in range(0, len(batters)):
				batters[j] = batters[j].split(str(j+1) + ".")[1].split(",")[0].replace(" ", "")
				k = int(len(batters[j])/2)
				batters[j] = batters[j][0:k]

			batters = pd.DataFrame(batters)

			team_temp = batters.columns[0].replace("Hitters", "").strip()
			teams.append(team_temp)

			batters.columns = ["Abreviated_Name"]
			batters["Full_Name"] = ""

			#Jr. bug fix
			fam = batters["Abreviated_Name"].str.split(".", expand = True)
			if None in list(fam.iloc[:,-1]):
				fam.drop(fam.columns[len(fam.columns) - 1], axis = 1, inplace = True)


			batters["Family_Name"] = list(fam.iloc[:,-1])

			#Get full names
			for j in range(0, len(batters)):
				index = np.where(family_names == batters.at[j, "Family_Name"])[0]

				if len(index) > 0:
					m = int(index[0])
					batters.at[j, "Full_Name"] = players[m]
					family_names = np.delete(family_names, m)
					players = np.delete(players, m)



			batters["Team"] = team_temp

			pitchers = pd.DataFrame(tables[2*i + 1]).iloc[0,0].split("  ")[0].replace(" ", "")
			pitchers = pd.DataFrame([pitchers], columns = ["Abreviated_Name"])

			#Jr. bug fix
			fam = pitchers["Abreviated_Name"].str.split(".", expand = True)
			if None in list(fam.iloc[:,-1]):
				fam.drop(fam.columns[len(fam.columns) - 1], axis = 1, inplace = True)	


			pitchers["Family_Name"] = list(fam.iloc[:,-1])
			pitchers["Full_Name"] = ""

			index = np.where(family_names == pitchers.at[0, "Family_Name"])[0]
			if len(index) > 0:
				m = int(index[0])
				pitchers.at[0, "Full_Name"] = players[m]
				family_names = np.delete(family_names, m)
				players = np.delete(players, m)

			pitchers["Team"] = team_temp

			if len(bat) == 0:
				bat = batters
			else:
				bat = bat.append(batters,  ignore_index = True)

			if len(pitch) == 0:
				pitch = pitchers
			else:
				pitch = pitch.append(pitchers,  ignore_index = True)



		teams = np.array(teams)

		#Extract moneyline odds 
		current_moneylines = []

		for i in range(0, int(len(moneylines)/4)):

			temp = moneylines[4*i].find_all("p", class_ = "foot-stat-value")

			for j in range(0,2):
				try:
					current_moneylines.append(int(temp[j].find_all("span")[0].text))
				except:
					current_moneylines.append(100)

		current_moneylines = np.array(current_moneylines)



		#Combine betting data
		index_home = np.array(list(range(1,len(teams),2))).astype(int)
		index_away = np.array(list(range(0,len(teams),2))).astype(int)


		betting = pd.DataFrame(np.transpose([teams[index_home], 
								current_moneylines[index_home], 
								teams[index_away], 
								current_moneylines[index_away]]),
								columns = ["Team_Home", "MoneyLine_Home", "Team_Away", "MoneyLine_Away"])

		betting["Odds_Home"] = 1/2
		betting["Odds_Away"] = 1/2

		betting["MoneyLine_Home"] = betting["MoneyLine_Home"].astype(int)
		betting["MoneyLine_Away"] = betting["MoneyLine_Away"].astype(int)

		for i in range(0, len(betting)):

			if betting.at[i, "MoneyLine_Home"] >= 0:
				R = (100 + betting.at[i, "MoneyLine_Home"]) / 100
			else:
				R = -(100 - betting.at[i, "MoneyLine_Home"]) / betting.at[i, "MoneyLine_Home"]
				
			betting.at[i, "Odds_Home"] = 1/R

			if betting.at[i, "MoneyLine_Away"] >= 0:
				R = (100 + betting.at[i, "MoneyLine_Away"]) / 100
			else:
				R = -(100 - betting.at[i, "MoneyLine_Away"]) / betting.at[i, "MoneyLine_Away"]
				
			betting.at[i, "Odds_Away"] = 1/R	


		betting["Returns_Home"] = 1 / betting["Odds_Home"] - 1
		betting["Returns_Away"] = 1 / betting["Odds_Away"] - 1		

		betting["OverOdds"] = betting["Odds_Home"] + betting["Odds_Away"] - 1


		#Add Date
		bat["Date"] = date
		pitch["Date"] = date
		betting["Date"] = date

		bat = self.Fix_Team_Names(bat, "City")
		pitch = self.Fix_Team_Names(pitch, "City")
		betting = self.Fix_Team_Names(betting, "City")



		def find_name(frame, row, all_names):

			dummy = np.char.lower(all_names)

			f_name = str(frame.at[row, "Full_Name"]) + str(frame.at[row, "Team"])

			index = np.where(dummy == f_name.lower())[0]

			if len(index) != 0:
				return f_name

			p_name = str(frame.at[row, "Family_Name"]) + str(frame.at[row, "Team"])
			p_name = p_name.lower()

			temp = str(frame.at[row, "Family_Name"])
			family_name = "".join([x for x in temp if x.isalpha()])

			family_name = family_name.lower()
			team_name = str(frame.at[row, "Team"]).lower()

			m = []
			for i in range(0, len(all_names)):
				temp = "".join([x for x in dummy[i] if x.isalpha()])

				if (family_name in temp) and (team_name in temp):
					m.append(i) 
					

			if len(m) == 0:
				return ""

			out = ""

			for i in m:
				temp = [x for x in all_names[i][0:len(all_names[i]) - 3] if x.isupper()]
				initials = temp[0] + "." 
				abreviated_name = initials + str(frame.at[row, "Family_Name"])

				if abreviated_name[-2:] == "Jr":
					abreviated_name = abreviated_name + "."

				if abreviated_name == str(frame.at[row, "Abreviated_Name"]):
					out = all_names[i]
					break


			if frame.at[row, "Full_Name"] == "KikeHernandez":
				return "EnriqueHernandez" + str(frame.at[row, "Team"])

			if frame.at[row, "Full_Name"] == "A.Toro-Hernandez":
				return "AbrahamToro" + str(frame.at[row, "Team"])				

			return out




		print("Matching names (bat) ...")

		#Match names with the original database
		path_check = self.paths[0] + "/Clean_Data/FanGraphs_Box_Scores.csv"
		if not path.exists(path_check):
			sys.exit("Missing file:" + "\t" + path_check)

		batters = pd.read_csv(path_check)["Name"]
		batters = np.array(list(set(list(batters))))


		bat["Name_Key"] = ""
		for i in tqdm(range(0, len(bat))):
			bat.at[i, "Name_Key"] = find_name(bat, i, batters)


		print("Matching names (pitch) ...")

		path_check = self.paths[1] + "/Clean_Data/FanGraphs_Box_Scores.csv"
		if not path.exists(path_check):
			sys.exit("Missing file:" + "\t" + path_check)

		pitchers = pd.read_csv(path_check)["Name"]
		pitchers = np.array(list(set(list(pitchers))))

		pitch["Name_Key"] = ""
		for i in tqdm(range(0, len(pitch))):
			pitch.at[i, "Name_Key"] = find_name(pitch, i, pitchers)



		print("Saving ...")

		#Save
		path_check = self.paths[3] + "/Predicted_Lineups"
		if not path.exists(path_check):
			os.mkdir(path_check)

		path_check = self.paths[3] + "/Predicted_Lineups/" + date 
		if not path.exists(path_check):
			os.mkdir(path_check)

		bat.to_csv(path_check + "/Bat.csv", index = False)
		pitch.to_csv(path_check + "/Pitch.csv", index = False)
		betting.to_csv(path_check + "/Moneyline.csv", index = False)

		print("Data avaible at: " + "\t" +  path_check)




	def Merge_Predicted_Lineups(self):

		path_check = self.paths[3] + "/Predicted_Lineups"
		if not path.exists(path_check):
			sys.exit("Missing directory at:" + "\t" + path_check)

		files_ext = [x for x in os.listdir(path_check) if "-" in x]
		if len(files_ext) == 0:
			sys.exit("No files to process:" + "\t" + path_check)

		bat = []
		pitch = []
		moneylines = []

		for ext in files_ext:

			path_load = path_check + "/" + ext

			if len(bat) == 0:
				bat = pd.read_csv(path_load + "/Bat.csv")
			else:
				bat = bat.append(pd.read_csv(path_load + "/Bat.csv"), ignore_index=True)

			if len(pitch) == 0:
				pitch = pd.read_csv(path_load + "/Pitch.csv")
			else:
				pitch = pitch.append(pd.read_csv(path_load + "/Pitch.csv"), ignore_index=True)		

			if len(moneylines) == 0:
				moneylines = pd.read_csv(path_load + "/Moneyline.csv")
			else:
				moneylines = moneylines.append(pd.read_csv(path_load + "/Moneyline.csv"), ignore_index=True)

		def find_name(frame, row, all_names):

			dummy = np.char.lower(all_names)

			f_name = str(frame.at[row, "Full_Name"]) + str(frame.at[row, "Team"])

			index = np.where(dummy == f_name.lower())[0]

			if len(index) != 0:
				return f_name

			p_name = str(frame.at[row, "Family_Name"]) + str(frame.at[row, "Team"])
			p_name = p_name.lower()

			temp = str(frame.at[row, "Family_Name"])
			family_name = "".join([x for x in temp if x.isalpha()])

			family_name = family_name.lower()
			team_name = str(frame.at[row, "Team"]).lower()

			m = []
			for i in range(0, len(all_names)):
				temp = "".join([x for x in dummy[i] if x.isalpha()])

				if (family_name in temp) and (team_name in temp):
					m.append(i) 
					

			if len(m) == 0:
				return ""

			out = ""

			for i in m:
				temp = [x for x in all_names[i][0:len(all_names[i]) - 3] if x.isupper()]
				initials = temp[0] + "." 
				abreviated_name = initials + str(frame.at[row, "Family_Name"])

				if abreviated_name[-2:] == "Jr":
					abreviated_name = abreviated_name + "."

				if abreviated_name == str(frame.at[row, "Abreviated_Name"]):
					out = all_names[i]
					break


			if frame.at[row, "Full_Name"] == "KikeHernandez":
				return "EnriqueHernandez" + str(frame.at[row, "Team"])

			if frame.at[row, "Full_Name"] == "A.Toro-Hernandez":
				return "AbrahamToro" + str(frame.at[row, "Team"])				

			return out



		#Find missing names
		bat_na = np.where(bat["Name_Key"].isnull())[0]
		if len(bat_na) > 0:

			path_check = self.paths[0] + "/Clean_Data/FanGraphs_Box_Scores.csv"
			batters = pd.read_csv(path_check)["Name"]
			batters = np.array(list(set(list(batters))))

			for i in tqdm(bat_na):
				bat.at[i, "Name_Key"] = find_name(bat, i, batters) 



		pitch_na = np.where(pitch["Name_Key"].isnull())[0]
		if len(pitch_na) > 0:

			path_check = self.paths[1] + "/Clean_Data/FanGraphs_Box_Scores.csv"
			pitchers = pd.read_csv(path_check)["Name"]
			pitchers = np.array(list(set(list(pitchers))))	

			for i in tqdm(pitch_na):
				pitch.at[i, "Name_Key"] = find_name(pitch, i, pitchers) 



		path_save = self.paths[3] + "/Predicted_Lineups"
		bat.to_csv(path_save + "/All_Batters.csv", index = False)
		pitch.to_csv(path_save + "/All_Pitchers.csv", index = False)
		moneylines.to_csv(path_save + "/All_Moneylines.csv", index = False)




	def Billet_Loto_Quebec(self):

		date = datetime.strftime(datetime.now(), "%Y-%m-%d")
		path_check = self.paths[3] + "/Predicted_Lineups/" + "Loto_Quebec_" + date + "/Billet.csv" 
		if path.exists(path_check):
			sys.exit("Forbiden: Cannot overwrite Billet.csv  ----  File is final.")

		url = "https://miseojeu.lotoquebec.com/fr/offre-de-paris/baseball/mlb/matchs?idAct=11"

		headers = {
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36',
    	}

		print("Accessing Loto-Quebec website...")

		#Obtain page data
		html = requests.get(url , stream = True, headers = headers).content
		try:
			tables = pd.read_html(html)
			soup = BeautifulSoup(html)
		except:
			sys.exit("Error: No bets found   ----   Too early, or no games today.")

		#Obtain moneylines
		moneylines = [x for x in tables if len(x.columns) == 4]
		moneylines = [x for x in moneylines if "Baseball  MLB" in x.iloc[0,1]]

		billet = pd.DataFrame([moneylines[0].iloc[0,1], moneylines[0].iloc[0,2]]).T

		for x in moneylines[1:]:
			temp = pd.DataFrame([x.iloc[0,1], x.iloc[0,2]]).T

			if "pt(s)" in temp.iloc[0,0]:
				break
			
			else:
				billet = billet.append(temp, ignore_index = True)

		billet.columns = ["Home", "Away"]

		teams = []
		returns = []

		for j in range(0, 2):

			temp = billet.iloc[:,j]
			nm = temp.str.split("  ")

			t = []
			r = []
			for i in range(0, len(temp)):

				t.append(nm[i][2])
				r.append(float(nm[i][3].replace(",", ".")))


			teams.append(t)
			returns.append(r)


		out = pd.DataFrame([teams[1], returns[1], teams[0], returns[0]]).T
		out.columns = ["Team_Home", "Factor_Home", "Team_Away", "Factor_Away"]
		out["Date"] = str(datetime.now()).split(" ")[0]

		path_check = self.paths[3] + "/Predicted_Lineups/" + "Loto_Quebec_" + out["Date"][0] 
		if not path.exists(path_check):
			os.mkdir(path_check)

		out.to_csv(path_check + "/Billet.csv", index = False)

		print("Done.")



	def Ajouter_Lineups(self):

		date = datetime.strftime(datetime.now(), "%Y-%m-%d")
		path_check = self.paths[3] + "/Predicted_Lineups/" + "Loto_Quebec_" + date
		if not path.exists(path_check):
			sys.exit("Aucun Billet.")

		billet = pd.read_csv(path_check + "/Billet.csv")
		original = billet[["Team_Home", "Team_Away"]].copy()
		original.columns = ["LotoQ_Symbol_Home", "LotoQ_Symbol_Away"]
		billet = billet.join(original)

		#Translate team names
		billet = self.Fix_Team_Names(billet, "City")

		#Attempt to obtain predicted lineups
		url = "https://www.rotowire.com/baseball/daily-lineups.php"
		headers = {
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36',
    	}

		print("Accessing: " + url +  " ...")

		#Extract projected roosters
		html = requests.get(url, headers = headers).content
		soup = BeautifulSoup(html)

		#Extract the rooster html object
		tables = soup.find_all("div", class_ = "lineup__box")
		#Filter garbage
		tables = [x for x in tables if len(x.find_all("div", class_ = "lineup__abbr")) == 2 and len(x.find_all("li", {"class" : "lineup__status"})) == 2]

		if len(tables) == 0:
			sys.exit("No predicted lineups found.")

		print(str(len(tables)) + " Lineups found...")

		#Initialize containers
		teams = []
		bat = []
		pitch = []

		#Retrieve data
		ID = 0
		for x in tables:

			frame_bat = pd.DataFrame(index = np.arange(0,9), 
										columns = ["Batter_Home", "Batter_Away"])

			frame_pitch = pd.DataFrame(index = np.arange(0,1), 
										columns = ["Pitcher_Home", "Pitcher_Away"])	

			frame_team = pd.DataFrame(index = np.arange(0,1), 
										columns = ["Team_Home", "Team_Away"])				

			home = x.find("ul", class_ = "lineup__list is-home")
			away = x.find("ul", class_ = "lineup__list is-visit")

			data = [home, away]

			for i in range(0,2):

				#Batter names
				batters = data[i].find_all("li", class_ = "lineup__player")
				for j in range(0, len(batters)):
					frame_bat.iloc[j,i] = batters[j].text.split("\n")[2]

				#Starting pitcher names
				pitchers = data[i].find("div", class_ = "lineup__player-highlight-name").text.split("\n")[1]
				frame_pitch.iloc[0,i] = pitchers


			#Teams
			frame_team.iloc[0,0] = x.find("div", class_ = "lineup__team is-home").text.split("\n")[2]
			frame_team.iloc[0,1] = x.find("div", class_ = "lineup__team is-visit").text.split("\n")[2]

			frame_bat["Team_Home"] = str(frame_team.iloc[0,0])
			frame_bat["Team_Away"] = str(frame_team.iloc[0,1])

			frame_pitch["Team_Home"] = str(frame_team.iloc[0,0])
			frame_pitch["Team_Away"] = str(frame_team.iloc[0,1])

			
			#Tag matches
			frame_team["ID"] = ID
			frame_bat["ID"] = ID
			frame_pitch["ID"] = ID

			#Update ID
			ID += 1

			#####
			#CHECK IF THE LINEUP ARE EXPECTED OR CONFIRMED
			status = x.find_all("li", {"class" : "lineup__status"})

			frame_team["Lineup_Away"] = status[0]["class"][-1]
			frame_bat["Lineup_Away"] = status[0]["class"][-1]
			frame_pitch["Lineup_Away"] = status[0]["class"][-1]	



			frame_team["Lineup_Home"] = status[1]["class"][-1]
			frame_bat["Lineup_Home"] = status[1]["class"][-1]
			frame_pitch["Lineup_Home"] = status[1]["class"][-1]			


			#Append results
			if len(teams) == 0:
				teams = frame_team
			else:
				teams = teams.append(frame_team, ignore_index = True)

			if len(bat) == 0:
				bat = frame_bat
			else:
				bat = bat.append(frame_bat, ignore_index = True)

			if len(pitch) == 0:
				pitch = frame_pitch
			else:
				pitch = pitch.append(frame_pitch, ignore_index = True)			


		#Fix team names
		teams = self.Fix_Team_Names(teams, "City")
		bat = self.Fix_Team_Names(bat, "City")
		pitch = self.Fix_Team_Names(pitch, "City")

		print(teams)


		#Fix player names
		print("Translating names to FanGraph values ...")

		path_check = self.paths[0] + "/Clean_Data/FanGraphs_Box_Scores.csv"
		batters = pd.read_csv(path_check)["Name"]
		batters = np.array(list(set(list(batters))))

		path_check = self.paths[1] + "/Clean_Data/FanGraphs_Box_Scores.csv"
		pitchers = pd.read_csv(path_check)["Name"]
		pitchers = np.array(list(set(list(pitchers))))


		def find_name(name, team, all_names):

			current = "None"

			dummy = np.char.lower(all_names)

			family_name = name.split(" ")[-1]
			first_letter = name[0]

			while True:

				#First Search
				search_val = (name.replace(" ", "") + team).lower()
				matches = np.where(dummy == search_val)[0]
				if len(matches) > 0:
					break

				#Second search, by family name
				search_val = (family_name + team).lower()
				matches = []
				for i in range(0, len(dummy)):
					if search_val in dummy[i]:
						matches.append(i)

				if len(matches) > 0:
					break

				#Third search, with no alphanumerics
				search_val = "".join([x for x in search_val if x.isalpha()])
				matches = []
				for i in range(0, len(dummy)):
					if search_val in "".join([x for x in dummy[i] if x.isalpha()]):
						matches.append(i)	

				if len(matches) > 0:
					break	


				#Added: family name, no team
				search_val = (family_name).lower()	
				matches = []
				for i in range(0, len(dummy)):
					if search_val in "".join([x for x in dummy[i] if x.isalpha()]):
						matches.append(i)	

				if len(matches) > 0:
					break						


				#Added: search for junior
				search_val = (family_name + "jr").lower()	
				matches = []
				for i in range(0, len(dummy)):
					if search_val in "".join([x for x in dummy[i] if x.isalpha()]):
						matches.append(i)	

				if len(matches) > 0:
					break	



				#Alternative search, with possible 2nd family name
				family_name = name.split(" ")[-2]

				#3rd search, by family name
				search_val = (family_name + team).lower()
				matches = []
				for i in range(0, len(dummy)):
					if search_val in dummy[i]:
						matches.append(i)

				if len(matches) > 0:
					break	

				#4th, with no alphanumerics
				search_val = "".join([x for x in search_val if x.isalpha()])
				matches = []
				for i in range(0, len(dummy)):
					if search_val in "".join([x for x in dummy[i] if x.isalpha()]):
						matches.append(i)			

				if len(matches) > 0:
					break	

				break


			if len(matches) > 0:
				for i in matches:
					if all_names[i][0] == first_letter and all_names[i][-3:] == team:
						current = all_names[i]

			return current


		#Initialize copies, esier to test on...
		fangraph_bat = bat.copy()
		fangraph_pitch = pitch.copy()

		for j in tqdm(range(0, len(bat))):
			fangraph_bat.iloc[j, 0] = find_name(bat.at[j, "Batter_Home"], bat.at[j, "Team_Home"], batters)
			fangraph_bat.iloc[j, 1] = find_name(bat.at[j, "Batter_Away"], bat.at[j, "Team_Away"], batters)

		for j in tqdm(range(0, len(pitch))):
			fangraph_pitch.iloc[j, 0] = find_name(pitch.at[j, "Pitcher_Home"], pitch.at[j, "Team_Home"], pitchers)
			fangraph_pitch.iloc[j, 1] = find_name(pitch.at[j, "Pitcher_Away"], pitch.at[j, "Team_Away"], pitchers)


		rmv = []
		#Remove matches where the starting pitcher wasn't found in the database
		pitch_missing = np.where(np.logical_or(fangraph_pitch["Pitcher_Home"] == "None", fangraph_pitch["Pitcher_Away"] == "None"))[0]
		if len(pitch_missing) > 0:
			for x in pitch_missing:
				rmv.append(fangraph_pitch.at[x, "ID"])	

		rmv = list(set(list(rmv)))	

		if len(rmv)	> 0:
			index = [x for x in np.arange(0, len(fangraph_bat)) if fangraph_bat.at[x, "ID"] not in rmv]
			fangraph_bat = fangraph_bat.iloc[index].reset_index(drop = True)

			index = [x for x in np.arange(0, len(fangraph_pitch)) if fangraph_pitch.at[x, "ID"] not in rmv]
			fangraph_pitch = fangraph_pitch.iloc[index].reset_index(drop = True)

			index = [x for x in np.arange(0, len(teams)) if teams.at[x, "ID"] not in rmv]
			teams = teams.iloc[index].reset_index(drop = True)	



		#Keep track of how many batters couldn't be found in the database	

		teams["Batters_Missing_Home"] = 0
		teams["Batters_Missing_Away"] = 0

		bat_missing = np.where(np.logical_or(fangraph_bat["Batter_Home"] == "None", fangraph_bat["Batter_Away"] == "None"))[0]
		if len(bat_missing) > 0:

			for i in bat_missing:

				i_team = np.where(teams["ID"] == fangraph_bat.at[i, "ID"])[0][0]

				if fangraph_bat.at[i, "Batter_Home"] == "None":
					teams.at[i_team, "Batters_Missing_Home"] += 1

				if fangraph_bat.at[i, "Batter_Away"] == "None":
					teams.at[i_team, "Batters_Missing_Away"] += 1				

		teams["Batters_Missing_Total"] = teams["Batters_Missing_Home"] + teams["Batters_Missing_Away"]



		#Match billet and teams
		billet = pd.merge(billet, teams, how = "inner", on = ["Team_Home", "Team_Away"]).drop_duplicates(["Team_Home", "Team_Away"],keep= 'first')

		#Keep roosters
		index = [x for x in np.arange(0, len(fangraph_bat)) if fangraph_bat.at[x, "ID"] in list(billet["ID"])]
		fangraph_bat = fangraph_bat.iloc[index].reset_index(drop = True)

		index = [x for x in np.arange(0, len(fangraph_pitch)) if fangraph_pitch.at[x, "ID"] in list(billet["ID"])]
		fangraph_pitch = fangraph_pitch.iloc[index].reset_index(drop = True)


		#Add metrics
		billet["Returns_Home"] = billet["Factor_Home"] - 1
		billet["Returns_Away"] = billet["Factor_Away"] - 1

		billet["Odds_Home"] = 1 / billet["Factor_Home"] 
		billet["Odds_Away"] = 1 / billet["Factor_Away"] 

		billet["OverOdds"] = billet["Odds_Home"] + billet["Odds_Away"] - 1

		billet["Odds_Home_FAIR"] = billet["Odds_Home"] / (billet["Odds_Home"] + billet["Odds_Away"])
		billet["Odds_Away_FAIR"] = billet["Odds_Away"] / (billet["Odds_Home"] + billet["Odds_Away"])	

		#Add date
		fangraph_bat["Date"] = date
		fangraph_pitch["Date"] = date

		#Save
		path_save = self.paths[3] + "/Predicted_Lineups/" + "Loto_Quebec_" + date + "/"
		billet.to_csv(path_save + "Billet_Final.csv", index = False)
		fangraph_bat.to_csv(path_save + "Bat.csv", index = False)
		fangraph_pitch.to_csv(path_save + "Pitch.csv", index = False)

		print(billet)




	def Assemble_Billet_Backtesting_Loto_Quebec(self):

		path_dir = self.paths[3] + "/Predicted_Lineups/"
		paths = [x for x in os.listdir(path_dir) if "Loto_Quebec" in x and ".csv" not in x]

		if len(paths) == 0:
			sys.exit("Aucun billet...")

		billet = []
		bat = []
		pitch = []

		for x in paths:
			if len(billet) == 0:
				billet = pd.read_csv(path_dir + x + "/Billet_Final.csv")
			else:
				billet = billet.append(pd.read_csv(path_dir + x + "/Billet_Final.csv"), ignore_index = True)

			if len(bat) == 0:
				bat = pd.read_csv(path_dir + x + "/Bat.csv")
			else:
				bat = bat.append(pd.read_csv(path_dir + x + "/Bat.csv"), ignore_index = True)

			if len(pitch) == 0:
				pitch = pd.read_csv(path_dir + x + "/Pitch.csv")
			else:
				pitch = pitch.append(pd.read_csv(path_dir + x + "/Pitch.csv"), ignore_index = True)				



		path_save = self.paths[3] + "/Predicted_Lineups/"
		billet.to_csv(path_save + "Historique_Loto_Quebec_Moneylines.csv", index = False)
		bat.to_csv(path_save + "Historique_Loto_Quebec_Bat.csv", index = False)
		pitch.to_csv(path_save + "Historique_Loto_Quebec_Pitch.csv", index = False)

		print(billet)
		

	def Evaluer_Billet_Adj(self, n):

		date = datetime.strftime(datetime.now(), "%Y-%m-%d")
		path_check = self.paths[0].replace("Bat", "") + "Regression/" + str(n) + "/Betting_Fitted_Odds.csv"

		if not path.exists(path_check):
			sys.exit("Missing file:" + "\t" + path_save)

		billet = pd.read_csv(path_check)

		index = np.where(billet["Date"] == date)[0]
		if len(index) == 0:
			sys.exit("No bets placed on:" + "\t" + date)

		billet = billet.iloc[index].reset_index(drop = True)

		#Check for live scores
		url = "https://www.mlb.com/scores" 

		headers = {
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36',
    	}

		print("Accessing " + url + "...")

		#Obtain page data
		html = requests.get(url, headers = headers).content
		soup = BeautifulSoup(html)

		tables = soup.find_all("div", {"data-test-mlb" : "singleGameContainer"})
		data = pd.DataFrame(index = np.arange(0, len(tables)), columns = ["Team_Home", "Team_Away", "Score_Home", "Score_Away", "Status"])
		data["Status"] = "Not_Final"

		for i in range(0, len(data)):

			try:

				teams = tables[i].find_all("div", {"data-mlb-test" : "teamNameLabel"})

				data.at[i, "Team_Away"] = teams[0].text
				data.at[i, "Team_Home"] = teams[1].text

				data.at[i, "Score_Away"] = int(tables[i].find_all("td", {"data-col" : "0", "data-row" : "0"})[0].text)
				data.at[i, "Score_Home"] = int(tables[i].find_all("td", {"data-col" : "0", "data-row" : "1"})[0].text)

				tables[i].find_all("div", {"data-mlb-test" : "gameStartTimesStateContainer"})

				if "Final" in tables[i].find_all("div", {"data-mlb-test" : "gameStartTimesStateContainer"})[0].text:

					data.at[i, "Status"] = "Final"



			except:

				continue


		rmv = np.where(data["Score_Away"].isnull())[0]
		if len(rmv) > 0:
			data = data.drop(rmv).reset_index(drop = True)

		rmv = np.where(data["Status"] == "Not_Final")[0]
		if len(rmv) > 0:
			data = data.drop(rmv).reset_index(drop = True)		

		data = self.Fix_Team_Names(data, "City")

		#Join tables
		billet = pd.merge(billet, data, how = "inner", on = ["Team_Home", "Team_Away"])
		billet["Win"] = 1

		loss = np.where(billet["Score_Home"] <= billet["Score_Away"])[0]
		if len(loss) > 0:
			billet.loc[loss, "Win"] = 0

		#Simulate 10$ bets
		#Arbitrage
		billet["10$_Bets_Delta"] = 10 * (billet["Factor_Home"] * billet["Win"] * billet["Linear_Home"] + billet["Factor_Away"] * (1 - billet["Win"]) * billet["Linear_Away"] - 1)
		billet["10$_Bets_Cumsum"] = np.cumsum(billet["10$_Bets_Delta"])
		billet["Arbitrage_Returns"] = billet["10$_Bets_Cumsum"] / (10 * (1 + np.arange(0, len(billet))))

		#Kelly
		money = len(billet) * 10
		scale = billet[["Kelly_Home", "Kelly_Away"]].sum().sum()

		billet.loc[:, ["Kelly_Home", "Kelly_Away"]] = billet.loc[:, ["Kelly_Home", "Kelly_Away"]] / scale

		billet["Kelly_Bets_Delta"] = money * (billet["Factor_Home"] * billet["Win"] * billet["Kelly_Home"] + billet["Factor_Away"] * (1 - billet["Win"]) * billet["Kelly_Away"] - (billet["Kelly_Home"] + billet["Kelly_Away"]))
		billet["Kelly_Bets_Cumsum"] = np.cumsum(billet["Kelly_Bets_Delta"])
		billet["Kelly_Returns"] = billet["Kelly_Bets_Cumsum"] / (10 * (1 + np.arange(0, len(billet))))

		print("####################################################################################")
		print("####################################################################################")
		print("###################" + "\t" + "\t" + "BETTING RESULTS"  + "\t" + "\t" + "############################")
		print("###################" + "\t" + "\t" + date + "\t" + "\t" + "############################")
		print("####################################################################################")
		print("####################################################################################")
		print("")
		print("")
		print("")
		print("")

		print(billet[["Team_Home", "Team_Away", "10$_Bets_Cumsum", "Arbitrage_Returns", "Kelly_Bets_Cumsum", "Kelly_Returns"]])

		billet.to_csv(self.paths[3] + "/Predicted_Lineups/Loto_Quebec_Gains_today.csv")



	def Reddit_Print_Billet_Final(self, n):

		date = datetime.strftime(datetime.now(), "%Y-%m-%d")
		path_check = self.paths[0].replace("Bat", "") + "Regression/" + str(n) + "/Betting_Fitted_Odds.csv"

		if not path.exists(path_check):
			sys.exit("Missing file:" + "\t" + path_save)

		billet = pd.read_csv(path_check)
		keep = np.where(billet["Date"] == date)[0]
		if len(keep) == 0:
			sys.exit("No bets made on that day.")

		billet = billet.iloc[keep].reset_index(drop = True)

		out = billet[["Team_Home", "Team_Away", "Factor_Home", "Factor_Away"]].copy()
		out.columns = ["Team_Home", "Team_Away", "R_LotoQ_Home", "R_LotoQ_Away"]
		out["R_Model_Home"] = (1 / billet["Odds"]).round(2)
		out["R_Model_Away"] = (1 / (1 - billet["Odds"])).round(2)
		out["Bet_on"] = out["Team_Home"]
		out["R+"] = out["R_LotoQ_Home"] - out["R_Model_Home"]

		bet_away = np.where(billet["Linear_Away"] == 1)[0]
		if len(bet_away) > 0:
			out.loc[bet_away, "Bet_on"] = out.loc[bet_away, "Team_Away"].copy()
			out.loc[bet_away, "R+"] = out.loc[bet_away, "R_LotoQ_Away"].copy() - out.loc[bet_away, "R_Model_Away"].copy()

		out["NaN"] = billet["Batters_Missing_Total"]
		out["Cf"] = "None"

		for i in range(0, len(out)):
			if billet.at[i, "Lineup_Home"] == "is-confirmed" and billet.at[i, "Lineup_Away"] == "is-confirmed":
				out.at[i, "Cf"] = "Both"
			elif billet.at[i, "Lineup_Home"] == "is-confirmed":
				out.at[i, "Cf"] = "Home"
			elif billet.at[i, "Lineup_Away"] == "is-confirmed":
				out.at[i, "Cf"] = "Away"				

		print(out)











